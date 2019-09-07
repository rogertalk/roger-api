# -*- coding: utf-8 -*-

import base64
import cgi
import collections
from datetime import date, datetime, timedelta
import difflib
import hashlib
import itertools
import json
import logging
import re
import struct
import time
import urllib
import zlib

from google.appengine.api import mail, memcache, search, taskqueue, urlfetch
from google.appengine.datastore import datastore_query
from google.appengine.ext import ndb

from flask import Flask, g, request
import pytz

from roger import accounts, apple, apps, auth, bots, config, external, files, localize
from roger import models, notifs, push_service, ratelimit, report, services, slack_api
from roger import streams, threads, youtube
from roger.apps import utils
from roger_common import bigquery_api, convert, events, errors, flask_extras
from roger_common import identifiers, random


app = Flask(__name__)
apps.set_up(app)


app_toplevel = ndb.toplevel(app)


QUERY_HIGHEST_PAYING_ACCOUNTS = u"""
SELECT
  account_id,
  SUM(amount) AS total_amount
FROM
  roger_reporting.wallet_payment_v1 p
WHERE
  p.receiver_id = %d
GROUP BY
  1
ORDER BY
  2 DESC
LIMIT
  %d
"""


QUERY_TOP_ACCOUNTS_FIRST = u"""
SELECT
  account_id,
  COUNT(*) AS score
FROM
  roger_reporting.content_first_v1
WHERE
  timestamp >= TIMESTAMP("%s") AND
  timestamp < TIMESTAMP("%s")
GROUP BY
  1
ORDER BY
  2 DESC
LIMIT
  %d
"""


QUERY_TOP_ACCOUNTS_FIRST_TAG = u"""
SELECT
  account_id,
  COUNT(*) AS score
FROM
  roger_reporting.content_first_v1
WHERE
  tags = "%s" AND
  timestamp >= TIMESTAMP("%s") AND
  timestamp < TIMESTAMP("%s")
GROUP BY
  1
ORDER BY
  2 DESC
LIMIT
  %d
"""


QUERY_TOP_ACCOUNTS_PAYMENTS = u"""
SELECT
  receiver_id AS account_id,
  SUM(amount) AS score
FROM
  roger_reporting.wallet_payment_v1 p
WHERE
  p.receiver_id != p.account_id
  AND timestamp >= TIMESTAMP("%s")
  AND timestamp < TIMESTAMP("%s")
GROUP BY
  1
ORDER BY
  2 DESC
LIMIT
  %d
"""


QUERY_TOP_ACCOUNTS_VOTES = u"""
SELECT
  creator_id AS account_id,
  COUNT(*) AS score
FROM
  roger_reporting.content_vote_v1
WHERE
  account_id != creator_id AND
  timestamp >= TIMESTAMP("%s") AND
  timestamp < TIMESTAMP("%s")
GROUP BY
  1
ORDER BY
  2 DESC
LIMIT
  %d
"""


QUERY_TOP_ACCOUNTS_VOTES_TAG = u"""
SELECT
  creator_id AS account_id,
  COUNT(*) AS score
FROM
  roger_reporting.content_vote_v1
WHERE
  account_id != creator_id AND
  tags = "%s" AND
  timestamp >= TIMESTAMP("%s") AND
  timestamp < TIMESTAMP("%s")
GROUP BY
  1
ORDER BY
  2 DESC
LIMIT
  %d
"""


bigquery_client = bigquery_api.BigQueryClient.for_appengine(
    project_id=config.BIGQUERY_PROJECT,
    dataset_id=config.BIGQUERY_DATASET)


@app.route('/<version>/batch', methods=['POST'])
@flask_extras.json_service()
@auth.authed_request()
def post_batch(session):
    """
    Create or get a list of streams for the provided participant(s).

    Request:
        POST /v1/batch?participant=1234567890&participant=%2B16461234321
        Authorization: Bearer 5ZVhC41gMAqjpno3ELKc5pLtcAbQAPO1bWJf6hCMFW8T20s

    Response:
        {...}

    """
    # Disable this endpoint for now.
    return {'data': []}
    # TODO: Rewrite this endpoint to be a lot faster.
    participants = map(accounts.Resolver.parse, flask_extras.get_parameter_list('participant'))
    if not participants:
        raise errors.MissingArgument('At least one participant must be provided')
    # Convert participant information to account keys.
    result = []
    for account_key in (p.get_or_create_account_key() for p in participants):
        if account_key == session.account_key:
            # skip adding self
            continue
        stream = streams.get_or_create(session.account, [account_key], reason='batch')
        result.insert(0, stream)
    return {'data': result}


@app.route('/<version>/bots', methods=['GET'])
@flask_extras.json_service()
@auth.authed_request()
def get_bots(session):
    return {
        'data': services.get_connected_and_featured(session.account, category='bot')
    }


@app.route('/<version>/challenge', methods=['POST'])
@flask_extras.json_service()
def post_challenge():
    """
    Begins authentication process based on the provided identifier. If the identifier is
    an e-mail address or phone number, it will send a code via that medium.

    Request:
        POST /v1/challenge?identifier=%2B123456789

    Response
        {"challenge": "code"}

    """
    identifier = identifiers.clean(flask_extras.get_parameter('identifier'))
    if not identifier:
        raise errors.MissingArgument('A valid identifier is required')
    phone_call = flask_extras.get_flag('call')
    client_id, _ = auth.get_client_details()
    if not client_id:
        logging.warning('Failed to deduce client id')
        client_id = 'ios'
    challenger = auth.get_challenger(client_id, identifier, call=phone_call)
    # Set up a result.
    result = {'challenge': challenger.method}
    if g.api_version >= 30:
        result['account'] = None
        result['team'] = None
    # Don't even bother with these IP ranges.
    ip = request.remote_addr
    if ip and (ip.startswith('37.8.') or ip.startswith('82.205.')):
        time.sleep(random.random() / 4 + 0.2)
        return result
    # TODO: Clean up all this rate limiting logic.
    # Rate limit by identifier.
    cache_key = 'post_challenge:{}'.format(zlib.adler32(challenger.identifier))
    if memcache.get(cache_key):
        logging.warning('Request ignored because it is being throttled by identifier')
        return result
    # Rate limit by IP.
    if not ratelimit.spend('challenge', 'ip', str(request.remote_addr)):
        logging.warning('Request ignored because it is being throttled by IP')
        slack_api.message(channel='#abuse', text='Rate limit: {}'.format(request.remote_addr))
        return result
    # Rate limit by challenge type.
    if not ratelimit.spend('challenge', challenger.method):
        logging.warning('Request ignored because it is being throttled by challenge type')
        slack_api.message(channel='#abuse', text='Rate limit: {}'.format(challenger.method))
        return result
    challenger.challenge()
    report.challenge_request(challenger.identifier, challenge=challenger.method)
    if phone_call:
        memcache.set(cache_key, True, config.THROTTLE_CHALLENGE_CALL)
    else:
        memcache.set(cache_key, True, config.THROTTLE_CHALLENGE)
    if g.api_version >= 30:
        # TODO: Consider the data that we expose.
        account_key = models.Account.resolve_key(identifier)
        if account_key:
            result['account'] = account_key.get()
        result['team'] = challenger.team
    return result


@app.route('/<version>/challenge/respond', methods=['POST'])
@flask_extras.json_service()
def post_challenge_respond():
    """
    Tries to authenticate the user based on the identifier/code pair. If the user is
    already authenticated and not an owner of the identifier, it will be added to their
    account.

    Request:
        POST /v1/challenge/respond?identifier=%2B123456789&secret=1234
        - or -
        POST /v1/challenge/respond
        identifier=%2B123456789
        secret=1234

    Response:
        {
            "access_token": "RLDvsbckw7tJJCiCPzU9bF",
            "refresh_token": "pArhTbEs8ex1f79vAqxR2",
            "token_type": "bearer",
            "expires_in": 3600,
            "status": "active",
            "account": {
                "id": 12345678,
                "display_name": "Bob Brown",
                "aliases": ["bob"]
            }
        }

    """
    identifier = flask_extras.get_parameter('identifier')
    secret = flask_extras.get_parameter('secret')
    if not identifier:
        raise errors.MissingArgument('An identifier is required')
    if not secret:
        raise errors.MissingArgument('A secret is required')
    # Validate that the user entered the correct challenge response.
    client_id, _ = auth.get_client_details()
    if not client_id:
        logging.warning('Failed to deduce client id')
        client_id = 'ios'
    challenger = auth.get_challenger(client_id, identifier)
    try:
        challenger.validate(secret)
    except:
        report.user_login_failed(challenger.identifier, challenge=challenger.method)
        if g.api_version < 49:
            raise errors.InvalidArgument('An invalid secret was provided')
        raise
    # Check if the user is authenticated already.
    session = auth.get_session()
    # Perform an action based on whether the identifier already exists or not.
    if session:
        user = accounts.get_handler(session.account)
        logging.debug('Validated %r with a session for %s', identifier, user.account_id)
        if not user.has_identifier(identifier):
            # Attempt to add this identifier to the account.
            try:
                user.add_identifier(identifier)
            except errors.AlreadyExists:
                # Identifier taken by a different account.
                user = accounts.get_handler(identifier)
                logging.debug('%r belongs to account %s; switching session',
                              identifier, user.account_id)
    else:
        user = accounts.get_or_create(identifier)
    # Report that the user successfully logged in.
    report.user_logged_in(user.account_id, auth_identifier=challenger.identifier,
                          challenge=challenger.method)
    # Make it possible to request another code again.
    memcache.delete('post_challenge:{}'.format(zlib.adler32(challenger.identifier)))
    # Track logins.
    taskqueue.add(url='/_ah/jobs/track_login',
                  countdown=20,
                  params={'account_id': user.account_id,
                          'auth_identifier': challenger.identifier,
                          'client_id': client_id},
                  queue_name=config.INTERNAL_QUEUE)
    try:
        models.AccountEvent.create(user.account.key, 'Logged In', event_class='info')
    except:
        logging.exception('Failed to log event')
    return utils.set_up_session(user, participants=flask_extras.get_parameter_list('stream_participant'))


@app.route('/<version>/clients/<client_id>', methods=['GET'])
@flask_extras.json_service()
@auth.authed_request()
def get_clients_client_id(session, client_id):
    """Gets info about a third-party client."""
    # TODO: Consider security implications of this, and only share approved clients.
    return services.get_client(client_id)


@app.route('/<version>/code', methods=['POST'])
@flask_extras.json_service()
@auth.authed_request()
def post_code(session):
    # TODO: The authorization endpoint shouldn't have to call the API.
    client_id = flask_extras.get_parameter('client_id')
    if not client_id:
        raise errors.MissingArgument('A client_id is required')
    code = session.create_auth_code(client_id, flask_extras.get_parameter('redirect_uri'))
    return code


@app.route('/<version>/contacts', methods=['POST'])
@flask_extras.json_service()
@auth.authed_request()
def post_contacts(session):
    """
    Takes a list of identifiers and returns a map the ones
    that have accounts, and whether they're active.
    """
    def identity_keys(lines):
        for line in lines:
            try:
                identifier, identifier_type = identifiers.parse(line)
                if identifier_type not in (identifiers.EMAIL, identifiers.PHONE):
                    continue
                yield ndb.Key('Identity', identifier)
            except:
                continue
    lines = request.data.strip().split('\n')
    futures = ndb.get_multi_async(identity_keys(lines),
                                  read_policy=ndb.EVENTUAL_CONSISTENCY)
    account_map = {}
    for i, future in enumerate(futures):
        try:
            identity = future.get_result()
        except:
            logging.warning('Erroneous contact %r', lines[i])
            continue
        if not identity or not identity.account:
            continue
        if g.api_version >= 10:
            account_map[identity.key.id()] = {
                'id': identity.account.id(),
                'active': identity.is_active,
            }
        else:
            if not identity.is_active:
                continue
            identifier, identifier_type = identifiers.parse(identity.key.id())
            if identifier_type == identifiers.EMAIL:
                account_map[identifiers.email(identifier)] = identity.account.id()
            else:
                account_map[identifier] = identity.account.id()
    logging.debug('Parsed %d lines into %d contact details, %d of which had accounts',
        len(lines), len(futures), len(account_map))
    return {'map': account_map}


@app.route('/<version>/content', methods=['GET'])
@flask_extras.json_service()
def get_content():
    slug = flask_extras.get_parameter('slug')
    url = flask_extras.get_parameter('url')
    if bool(slug) == bool(url):
        raise errors.InvalidArgument('Specify one of "slug" or "url" parameters')
    q = models.Content.query()
    if slug:
        cache_id = 'slug:' + _b64hash(slug)
        q = q.filter(models.Content.slug == slug)
    elif url:
        url, _ = utils.normalize_content_urls(url, None)
        cache_id = 'url:' + _b64hash(url)
        q = q.filter(models.Content.original_url == url)
    else:
        raise errors.ServerError()
    include_extras = flask_extras.get_flag('include_extras')
    session = auth.get_session()
    session_key = session.account_key if session else None
    # Try cache first.
    cache_key = 'content_%s_%s_%s' % (g.api_version, cache_id, 'extras' if include_extras else 'normal')
    cache_json = _content_cache_load(cache_key, session_key)
    if cache_json:
        return convert.Raw(cache_json)
    # Cache miss; get from datastore.
    content = q.get()
    if not content:
        raise errors.ResourceNotFound('Content not found')
    if session and content.creator in session.account.blocked_by:
        raise errors.ResourceNotFound('Content not found')
    creator, related_to, voted = content.decoration_info(
        include_creator=True,
        include_related=True,
        for_account_key=session_key)
    g.public_options.setdefault('include_extras', include_extras)
    result_dict = {
        'content': content,
        'creator': creator,
        'related_to': related_to,
        'voted': voted,
    }
    _content_cache_save(cache_key, result_dict)
    return result_dict


@app.route('/<version>/content', methods=['POST', 'PUT'])
@flask_extras.json_service()
@auth.authed_request()
def post_content(session):
    try:
        duration = int(flask_extras.get_parameter('duration'))
    except:
        raise errors.InvalidArgument('Invalid duration')
    tags = models.Content.parse_tags(flask_extras.get_parameter('tags') or '')
    url = flask_extras.get_parameter('url')
    if not models.Content.valid_upload_url(url):
        raise errors.InvalidArgument('Invalid URL')
    request_id = flask_extras.get_parameter('request_id')
    if request_id:
        try:
            request_id = int(request_id)
        except:
            raise errors.InvalidArgument('Invalid request_id')
    image = request.files.get('image')
    if image:
        path = files.upload(image.filename, image.stream, persist=True)
        thumb_url = files.storage_url(path)
    else:
        thumb_url = None
    content = models.Content.new(
        creator=session.account_key,
        duration=duration,
        tags=tags,
        thumb_url=thumb_url,
        title=flask_extras.get_parameter('title'),
        video_url=url,
        useragent=request.headers.get('User-Agent', 'unknown'))
    dedupe_key = flask_extras.get_parameter('dedupe')
    if dedupe_key:
        q = models.Content.query(models.Content.dedupe == dedupe_key)
        content_key = q.get(keys_only=True)
        if content_key:
            raise errors.AlreadyExists('That content has already been created',
                                       content_id=content_key.id())
        content.dedupe = dedupe_key
    country = request.headers.get('X-Appengine-Country')
    if country:
        content.add_tag('in ' + country, allow_restricted=True)
    if content.slug:
        logging.debug('Creating content with slug %r', content.slug)
    if not content.thumb_url:
        content.thumb_url = content.thumb_url_from_video_url()
    related_to_future = utils.get_or_create_content_from_request_async('original', get_if_needed=True)
    if request_id:
        request_future = models.ContentRequestPublic.get_by_id_async(request_id)
    else:
        request_future = None
    related_to_key, related_to = related_to_future.get_result()
    content.related_to = related_to_key
    if request_future:
        # Don't use "request" name due to global.
        req = request_future.get_result()
        if not req:
            raise errors.InvalidArgument('Invalid request_id')
        if req.content != related_to_key:
            raise errors.InvalidArgument('Reaction is not valid for specified request')
        content.request = req.key
    if content.is_public:
        content.became_public(session.account, related_to, first_time=True)
    content.put()
    # Perform additional bookkeeping.
    if content.is_public:
        _handle_content_became_public(session.account, content, related_to)
    futures = []
    # Transcode the video file to an HLS stream.
    s3_key = content.s3_key
    if s3_key:
        qs = urllib.urlencode({
            'content_id': str(content.key.id()),
            'file': s3_key,
        })
        future = ndb.get_context().urlfetch(
            headers={'Authorization': 'Bearer %s' % (session._access_token,)},
            method='POST',
            url=config.SERVICE_HLS_TRANSCODE + '?' + qs)
        futures.append(future)
    # Auto-posting to external networks.
    if flask_extras.get_flag('upload_to_youtube'):
        if session.account.has_service('youtube'):
            futures.append(_upload_to_youtube_async(session.account, content))
        else:
            logging.warning('User attempted to upload to YouTube without token')
    for future in futures:
        try:
            future.get_result()
        except:
            logging.exception('Exception while trying to post externally.')
    g.public_options.setdefault('include_extras', flask_extras.get_flag('include_extras'))
    return {
        'content': content,
        'creator': session.account,
        'related_to': related_to,
        'voted': False,
    }


@app.route('/<version>/content/batch', methods=['GET'])
@flask_extras.json_service()
def get_content_batch():
    # TODO: Utilize cache.
    try:
        content_ids = map(int, flask_extras.get_parameter_list('id'))
        assert len(content_ids) > 0
    except:
        raise errors.InvalidArgument('One more more "id" arguments must be provided as integers')
    content_list = ndb.get_multi(map(lambda cid: ndb.Key('Content', cid), content_ids))
    if not all(content_list):
        raise errors.ResourceNotFound('Content not found')
    session = auth.get_session()
    session_key = session.account_key if session else None
    lookup, votes = models.Content.decorate(content_list,
                                            include_creator=True,
                                            include_related=True,
                                            for_account_key=session_key)
    data = []
    for i, content in enumerate(content_list):
        result = {
            'content': content,
            'creator': lookup[content.creator],
            'related_to': lookup.get(content.related_to),
            'voted': votes[i] is not None,
        }
        data.append(result)
    return {'data': data}


@app.route('/<version>/content/<content_id>', methods=['GET'])
@flask_extras.json_service()
def get_content_content_id(content_id):
    try:
        content_id = int(content_id)
    except:
        raise errors.UnsupportedEndpoint()
    include_extras = flask_extras.get_flag('include_extras')
    session = auth.get_session()
    session_key = session.account_key if session else None
    # Try cache first.
    cache_key = 'content_%s_id:%d_%s' % (g.api_version, content_id, 'extras' if include_extras else 'normal')
    cache_json = _content_cache_load(cache_key, session_key)
    if cache_json:
        return convert.Raw(cache_json)
    # Cache miss; get from datastore.
    content = models.Content.get_by_id(content_id)
    if not content or not content.visible_by(session_key):
        raise errors.ResourceNotFound('Content not found')
    if session and content.creator in session.account.blocked_by:
        raise errors.ResourceNotFound('Content not found')
    creator, related_to, voted = content.decoration_info(
        include_creator=True,
        include_related=True,
        for_account_key=session_key)
    g.public_options.setdefault('include_extras', include_extras)
    result_dict = {
        'content': content,
        'creator': creator,
        'related_to': related_to,
        'voted': voted,
    }
    _content_cache_save(cache_key, result_dict)
    return result_dict


@app.route('/<version>/content/<content_id>', methods=['POST', 'PUT'])
@flask_extras.json_service()
@auth.authed_request()
def post_content_content_id(session, content_id):
    try:
        content_id = int(content_id)
    except:
        raise errors.UnsupportedEndpoint()
    content = models.Content.get_by_id(content_id)
    if not content:
        raise errors.ResourceNotFound('Content not found')
    if content.creator != session.account_key:
        raise errors.ForbiddenAction('You can only modify your own content')
    # Track if the content has ever been public before updating its tags.
    has_been_public = content.has_been_public
    was_public = content.is_public
    # TODO: Support updating more fields.
    image = request.files.get('image')
    if image:
        path = files.upload(image.filename, image.stream, persist=True)
        content.thumb_url = files.storage_url(path)
    tags = flask_extras.get_parameter('tags')
    if tags is not None:
        content.set_tags(tags)
        slug = content.slug_from_video_url()
        if content.is_public:
            content.slug = slug
            logging.debug('Setting slug to %r', slug)
        elif content.slug == slug:
            # Make the content private again if all public tags are removed.
            logging.debug('Removing slug because %s does not contain public tags', content.tags)
            content.slug = None
    title = flask_extras.get_parameter('title')
    if title is not None:
        content.title = title or None
    _, related_to, voted = content.decoration_info(
        include_related=True,
        for_account_key=session.account_key)
    if content.is_public:
        content.became_public(session.account, related_to, first_time=not has_been_public)
    content.put()
    if content.is_public and not has_been_public:
        # If the content has never been public before, check if it is now and notify.
        _handle_content_became_public(session.account, content, related_to)
    if not content.is_public and was_public:
        _handle_content_was_deleted(session.account, content, related_to)
    g.public_options.setdefault('include_extras', flask_extras.get_flag('include_extras'))
    return {
        'content': content,
        'creator': session.account,
        'related_to': related_to,
        'voted': voted,
    }


@app.route('/<version>/content/<content_id>/comments/', methods=['GET'])
@flask_extras.json_service()
def get_content_content_id_comments(content_id):
    try:
        content_id = int(content_id)
    except:
        raise errors.UnsupportedEndpoint()
    content_key = ndb.Key('Content', content_id)
    sort = flask_extras.get_parameter('sort') or 'offset'
    q = models.ContentComment.query(ancestor=content_key)
    if sort in ('created', 'threaded'):
        q = q.order(-models.ContentComment.created)
    elif sort == 'offset':
        q = q.order(models.ContentComment.offset)
        q = q.filter(models.ContentComment.offset >= 0)
    else:
        raise errors.InvalidArgument('Invalid sort value')
    cache_key = 'content_comments_%s_%d_%s' % (g.api_version, content_id, sort)
    result_json = memcache.get(cache_key)
    if result_json:
        logging.debug('Loaded cache key %r', cache_key)
        return convert.Raw(result_json)
    future = q.fetch_async()
    content = content_key.get()
    session = auth.get_session()
    session_key = session.account_key if session else None
    if not content or not content.visible_by(session_key):
        raise errors.ResourceNotFound('Content not found')
    if session and content.creator in session.account.blocked_by:
        raise errors.ResourceNotFound('Content not found')
    comments = future.get_result()
    lookup = {a.key: a for a in ndb.get_multi({c.creator for c in comments})}
    # For threaded sort, place replies under their parent comment and sort them ascending.
    if sort == 'threaded':
        comments_by_id = {}
        threads = collections.OrderedDict()
        for c in comments:
            c_id = c.key.id()
            comments_by_id[c_id] = c
            if not c.reply_to:
                if c_id not in threads:
                    threads[c_id] = []
                continue
            p_id = c.reply_to.id()
            if p_id in threads:
                threads[p_id].insert(0, c_id)
            else:
                threads[p_id] = [c_id]
        comments = []
        for p_id, replies in threads.iteritems():
            if p_id not in comments_by_id:
                logging.error('Could not find parent %r (%d replies)', p_id, len(replies))
                continue
            comments.append(comments_by_id[p_id])
            for c_id in replies:
                comments.append(comments_by_id[c_id])
    data = []
    for c in comments:
        creator = lookup[c.creator]
        if session and c.creator in session.account.blocked_by:
            # Don't show comments from users that have blocked viewer.
            continue
        if session and session_key in creator.blocked_by:
            # Don't show comments from users blocked by the viewer.
            continue
        data.append(c.public(creator=creator, version=g.api_version))
    result_json = convert.to_json({'data': data}, **g.public_options)
    cache_ttl = 3600
    memcache.set(cache_key, result_json, time=cache_ttl)
    logging.debug('Saved to cache key %r (ttl: %d)', cache_key, cache_ttl)
    return convert.Raw(result_json)


@app.route('/<version>/content/<content_id>/comments/<comment_id>', methods=['DELETE'])
@flask_extras.json_service()
@auth.authed_request()
def delete_content_content_id_comments_comment_id(session, content_id, comment_id):
    try:
        content_id = int(content_id)
    except:
        raise errors.UnsupportedEndpoint()
    content_key = ndb.Key('Content', content_id)
    comment_key = ndb.Key('Content', content_id, 'ContentComment', comment_id)
    content, comment = ndb.get_multi([content_key, comment_key])
    if not content:
        raise errors.ResourceNotFound('Content not found')
    if comment and session.account_key in (content.creator, comment.creator):
        if comment.creator == session.account_key:
            creator = session.account
        else:
            creator = comment.creator.get()
        # FIXME: Spamming this endpoint could reduce comment count to 0.
        futures = [
            comment.key.delete_async(),
            _change_content_comment_count_async(creator, comment, -1),
        ]
        if g.api_version:
            v = int(g.api_version)
            c = ndb.get_context()
            for x in (-1, 0, 1):
                futures += [
                    c.memcache_delete('content_comments_%s_%d_created' % (v + x, content_id)),
                    c.memcache_delete('content_comments_%s_%d_offset' % (v + x, content_id)),
                    c.memcache_delete('content_comments_%s_%d_threaded' % (v + x, content_id)),
                ]
        _wait_all(futures)
        return {'success': True}
    if not content.visible_by(session.account_key):
        raise errors.ResourceNotFound('Content not found')
    if content.creator in session.account.blocked_by:
        raise errors.ResourceNotFound('Content not found')
    if not comment:
        raise errors.ResourceNotFound('Comment not found')


@app.route('/<version>/content/<content_id>/comments/', methods=['PUT'])
@app.route('/<version>/content/<content_id>/comments/<int:offset>', methods=['PUT'])
@flask_extras.json_service()
@auth.authed_request()
def put_content_content_id_comments_offset(session, content_id, offset=-1):
    try:
        content_id = int(content_id)
    except:
        raise errors.UnsupportedEndpoint()
    content = models.Content.get_by_id(content_id)
    if not content or not content.visible_by(session.account_key):
        raise errors.ResourceNotFound('Content not found')
    if content.creator in session.account.blocked_by:
        raise errors.ResourceNotFound('Content not found')
    if offset < -1:
        offset = -1
    elif offset >= content.duration:
        offset = content.duration - 1
    text = flask_extras.get_parameter('text')
    if not text:
        raise errors.InvalidArgument('Invalid text')
    reply_to = flask_extras.get_parameter('reply_to')
    if reply_to:
        reply_to_key = ndb.Key('Content', content_id, 'ContentComment', reply_to)
        parent = reply_to_key.get()
        if not parent:
            raise errors.ResourceNotFound('Comment specified by "reply_to" not found')
        comment_id_hash_key = u'%d_%d_%s_%s' % (session.account_id, offset, reply_to, text)
    else:
        reply_to_key = None
        comment_id_hash_key = u'%d_%d_%s' % (session.account_id, offset, text)
    comment_id_hash_value = zlib.crc32(comment_id_hash_key.encode('utf-8'))
    comment_id = convert.to_base62(0x100000000 | (comment_id_hash_value & 0xFFFFFFFF))
    comment = models.ContentComment(
        id=comment_id,
        parent=content.key,
        creator=session.account_key,
        offset=offset,
        reply_to=reply_to_key,
        text=text)
    hub = notifs.Hub(content.creator)
    futures = [
        comment.put_async(),
        hub.emit_async(notifs.ON_CONTENT_COMMENT,
            comment=comment,
            commenter=session.account,
            content=content),
        _change_content_comment_count_async(session.account, comment, 1),
    ]
    # Notify users mentioned in the comment and/or previous commenters.
    # TODO: Add creator of `parent` (from `reply_to_key`) as another mention.
    task = taskqueue.Task(
        countdown=2,
        url='/_ah/jobs/content_comment_extras',
        params={'comment_id': comment_id,
                'commenter_id': session.account_key.id(),
                'content_id': content.key.id(),
                'creator_id': content.creator.id(),
                'mentions': ','.join(identifiers.find_mentions(text))})
    futures.append(_add_task_async(task, queue_name=config.INTERNAL_QUEUE))
    if g.api_version:
        v = int(g.api_version)
        c = ndb.get_context()
        for x in (-1, 0, 1):
            futures += [
                c.memcache_delete('content_comments_%s_%d_created' % (v + x, content_id)),
                c.memcache_delete('content_comments_%s_%d_offset' % (v + x, content_id)),
                c.memcache_delete('content_comments_%s_%d_threaded' % (v + x, content_id)),
            ]
    _wait_all(futures)
    return comment.public(creator=session.account, version=g.api_version)


@app.route('/<version>/content/<content_id>/flag', methods=['PUT'])
@flask_extras.json_service()
@auth.authed_request()
def post_content_content_id_flag(session, content_id):
    try:
        content_id = int(content_id)
    except:
        raise errors.UnsupportedEndpoint()
    logging.debug('Account %d flagged content %d', session.account_id, content_id)
    content = models.Content.get_by_id(content_id)
    if content:
        content.add_tag('flagged', allow_restricted=True)
        content.put()
        slack_api.message(channel='#abuse', hook_id='reactioncam', text='{} flagged {}'.format(
            slack_api.admin(session.account),
            slack_api.admin(content)))
    return {'success': True}


@app.route('/<version>/content/<content_id>/related/<tag>/', methods=['GET'])
@flask_extras.json_service()
def get_content_content_id_related_tag(content_id, tag):
    if tag not in ('featured', 'reaction'):
        raise errors.UnsupportedEndpoint()
    try:
        key = ndb.Key('Content', int(content_id))
    except:
        raise errors.UnsupportedEndpoint()
    sort = flask_extras.get_parameter('sort') or 'hot'
    try:
        limit = int(flask_extras.get_parameter('limit') or 50)
        assert 0 < limit <= 500
    except:
        raise errors.InvalidArgument('Invalid limit')
    session = auth.get_session()
    session_key = session.account_key if session else None
    cursor_urlsafe = flask_extras.get_parameter('cursor')
    is_first_page = not cursor_urlsafe
    if is_first_page:
        # Attempt to get the data from cache (first page only).
        cache_key = 'related_%s_%d_%s_%s_%d' % (g.api_version, key.id(), tag, sort, limit)
        cache_json = memcache.get(cache_key)
    else:
        cache_json = None
    if cache_json:
        logging.debug('Loaded cache key %r', cache_key)
        return convert.Raw(_load_and_inject_votes(cache_json, session_key))
    # Read the data from datastore.
    q = models.Content.query()
    q = q.filter(models.Content.related_to == key)
    q = q.filter(models.Content.tags == tag)
    if sort == 'hot':
        q = q.order(-models.Content.sort_index)
    elif sort == 'recent':
        q = q.order(-models.Content.created)
    elif sort == 'top':
        q = q.order(-models.Content.sort_bonus)
    else:
        raise errors.InvalidArgument('Invalid sort value')
    cursor = datastore_query.Cursor(urlsafe=cursor_urlsafe)
    content_list, next_cursor, more = q.fetch_page(limit, start_cursor=cursor)
    # Hide some content.
    content_list = _filter_content(content_list, hide_flagged=((tag, sort) == ('reaction', 'recent')))
    # Look up extra data for the content list.
    lookup, votes = models.Content.decorate(content_list,
                                            include_creator=True,
                                            for_account_key=session_key)
    next_cursor_urlsafe = next_cursor.urlsafe() if more else None
    # Build the result and a payload to cache.
    cache_data = []
    data = []
    for i, content in enumerate(content_list):
        # Special logic to hide non-approved reactions to public content requests.
        if content.request and 'is approved' not in content.tags:
            continue
        result = {
            'content': content,
            'creator': lookup[content.creator],
            'voted': votes[i] is not None,
        }
        data.append(result)
        if is_first_page:
            # Create a marker that marks the content id so we can replace it with vote data.
            cache_marker = config.CONTENT_CACHE_MARKER + str(content.key.id())
            cache_data.append(dict(result, voted=cache_marker))
    if is_first_page:
        cache_data = {'cursor': next_cursor_urlsafe, 'data': cache_data}
        cache_json = convert.to_json(cache_data, **g.public_options)
        if content_list and sort != 'recent':
            cache_ttl = min(int((datetime.utcnow() - content_list[0].created).total_seconds()), 86400)
        else:
            cache_ttl = 60
        memcache.set(cache_key, cache_json, time=cache_ttl)
        logging.debug('Saved to cache key %r (ttl: %d)', cache_key, cache_ttl)
    return {'cursor': next_cursor_urlsafe, 'data': data}


@app.route('/<version>/content/<content_id>/track', methods=['POST', 'PUT'])
@flask_extras.json_service()
def post_content_content_id_track(content_id):
    try:
        content_id = int(content_id)
    except:
        raise errors.UnsupportedEndpoint()
    content = models.Content.get_by_id(content_id)
    if not content:
        raise errors.ResourceNotFound('Content could not be found')
    try:
        data = json.loads(request.data.strip())
    except:
        raise errors.InvalidArgument('Invalid data')
    event = events.ContentActivityV1(
        content,
        identifier=data.get('id'),
        activity_type=data.get('type'))
    event.report()
    return {'success': True}


@app.route('/<version>/content/<content_id>/views', methods=['POST', 'PUT'])
@flask_extras.json_service()
def post_content_content_id_views(content_id):
    try:
        content_id = int(content_id)
    except:
        raise errors.UnsupportedEndpoint()
    user_agent = request.headers.get('User-Agent', '')
    user_info = '%s\n%s' % (
        user_agent,
        request.headers.get('X-Forwarded-For', request.remote_addr))
    user_hash = _b64hash(user_info)
    cache_key = 'content:%d:views:%s' % (content_id, user_hash)
    if memcache.get(cache_key):
        logging.debug('Ignoring duplicate view for key %r', cache_key)
        return {'success': True}
    content = models.Content.get_by_id(content_id)
    if not content or not content.is_public:
        raise errors.ResourceNotFound('Content not found')
    logging.debug('Counting view for key %r', cache_key)
    is_bot = 'ReactionCam/1337' in user_agent
    session = auth.get_session()
    content.add_view_count(session.account if session else None, is_bot=is_bot)
    content.put()
    memcache.set(cache_key, True)
    notifs.Hub(content.creator).emit(notifs.ON_CONTENT_VIEW, content=content)
    return {'success': True}


@app.route('/<version>/content/<content_id>/votes', methods=['POST', 'PUT'])
@flask_extras.json_service()
@auth.authed_request()
def post_content_content_id_votes(session, content_id):
    try:
        content_id = int(content_id)
    except:
        raise errors.UnsupportedEndpoint()
    content_key = ndb.Key('Content', content_id)
    vote_key = ndb.Key('Account', session.account_id, 'ContentVote', content_id)
    content, vote = ndb.get_multi([content_key, vote_key])
    if not content or not content.visible_by(session.account_key):
        raise errors.ResourceNotFound('Content not found')
    if content.creator in session.account.blocked_by:
        raise errors.ResourceNotFound('Content not found')
    if content.related_to:
        creator, related_to = ndb.get_multi([content.creator, content.related_to])
    else:
        creator, related_to = content.creator.get(), None
    if vote is None:
        is_bot = 'ReactionCam/1337' in request.headers.get('User-Agent', '')
        content.add_vote_count(session.account, is_bot=is_bot)
        vote = models.ContentVote(key=vote_key, content=content.key)
        handler = accounts.get_handler(creator)
        event = events.ContentVoteV1(
            account_key=session.account_key,
            content_id=content.key.id(),
            related_id=content.related_to.id() if content.related_to else None,
            creator_id=content.creator.id(),
            duration=content.duration / 1000,
            tags=content.tags,
            views=content.views,
            views_real=content.views_real,
            votes=content.votes,
            votes_real=content.votes_real)
        futures = ndb.put_multi_async([content, vote])
        futures += [
            handler.notifs.emit_async(notifs.ON_CONTENT_VOTE, content=content, voter=session.account),
            handler.add_vote_async(),
            event.report_async()]
        _wait_all(futures)
    return {
        'content': content,
        'creator': creator,
        'related_to': related_to,
        'voted': True,
    }


@app.route('/<version>/content/<content_id>/youtube', methods=['POST', 'PUT'])
@flask_extras.json_service()
@auth.authed_request()
def post_content_content_id_youtube(session, content_id):
    try:
        content_id = int(content_id)
    except:
        raise errors.UnsupportedEndpoint()
    content_key = ndb.Key('Content', content_id)
    content = content_key.get()
    if not content:
        raise errors.ResourceNotFound('Content not found')
    if content.creator != session.account_key:
        raise errors.ForbiddenAction('Must be creator of content')
    account = session.account
    futures = []
    channel_id = flask_extras.get_parameter('channel_id')
    if channel_id is not None:
        if channel_id == '':
            channel_id = None
        if channel_id != account.youtube_channel_id:
            account.youtube_channel_id = channel_id
            futures.append(account.put_async())
    youtube_id = flask_extras.get_parameter('youtube_id')
    if youtube_id is not None:
        if youtube_id == '':
            youtube_id = None
        if youtube_id != content.youtube_id:
            if content.youtube_id:
                event_future = models.AccountEvent.create_async(
                    session.account_key, 'YouTube Id Override',
                    event_class='warning',
                    properties={'ContentId': str(content.key.id()),
                                'NewVideoId': youtube_id,
                                'OldVideoId': content.youtube_id,
                                'OldVideoViews': content.youtube_views,
                                'WasBroken': content.youtube_broken})
                futures.append(event_future)
            content.set_youtube_id(youtube_id)
            futures.append(content.put_async())
            # Schedule a view count update 30 minutes from now.
            task = taskqueue.Task(
                countdown=1800,
                url='/_ah/jobs/update_youtube_views',
                params={'content_id': content.key.id()})
            futures.append(_add_task_async(task, queue_name=config.INTERNAL_QUEUE))
    if content.request and content.youtube_id:
        # Content is part of a request entry that may be waiting on YouTube.
        entry_future = models.ContentRequestPublicEntry.update_async(
            (content.request, account.key), content)
        futures.append(entry_future)
    else:
        entry_future = None
    if account.has_service('youtube'):
        if not account.youtube_subs:
            queue_subs_update = True
        elif account.youtube_subs_updated:
            time_since_update = datetime.utcnow() - account.youtube_subs_updated
            queue_subs_update = time_since_update > timedelta(hours=48)
        else:
            queue_subs_update = False
        if queue_subs_update:
            task = taskqueue.Task(
                url='/_ah/jobs/update_youtube_channel',
                params={'account_id': account.key.id()})
            futures.append(_add_task_async(task, queue_name=config.INTERNAL_QUEUE))
    event_future = models.AccountEvent.create_async(
        session.account_key, 'YouTube Id Callback',
        event_class='info',
        properties={'ChannelId': channel_id,
                    'ContentId': str(content.key.id()),
                    'VideoId': youtube_id})
    futures.append(event_future)
    _wait_all(futures)
    if entry_future:
        entry, did_change = entry_future.get_result()
        if did_change:
            hub = notifs.Hub(account)
            hub.emit(notifs.ON_PUBLIC_REQUEST_UPDATE, request_id=entry.request.id())
    return {'success': True}


@app.route('/<version>/content/search', methods=['GET'])
@flask_extras.json_service()
def get_content_search():
    query = flask_extras.get_parameter('query') or ''
    query = query.replace(u'\ufffc', u'')
    query = re.sub(r'\s{2,}', ' ', query.lower().strip())[:40]
    cache_key = 'search_%s_%s' % (g.api_version, base64.b64encode(query.encode('utf-8')))
    result_json = memcache.get(cache_key)
    if result_json:
        logging.debug('Loaded cache key %r', cache_key)
        return convert.Raw(result_json)
    result = {'data': [], 'total_count': 0}
    if query:
        try:
            s = search.Index('original2').search(query)
        except search.QueryError:
            raise errors.InvalidArgument('Invalid search query')
        except search.Error:
            logging.exception('Search error')
            raise errors.InvalidArgument('Invalid search query')
        def v(doc, key, default=None):
            try:
                field = doc.field(key)
            except ValueError:
                return default
            return field.value
        for doc in s.results:
            creator_id = v(doc, 'creator_id')
            if creator_id:
                creator = {
                    'id': int(creator_id),
                    'image_url': v(doc, 'creator_image_url') or None,
                    'username': v(doc, 'creator_username'),
                    'verified': v(doc, 'creator_verified') == 'Y',
                }
            else:
                creator = None
            result['data'].append({
                'id': int(doc.doc_id),
                'creator': creator,
                'duration': int(v(doc, 'duration', 0) * 1000),
                'original_url': v(doc, 'url'),
                'rank': doc.rank,
                'related_count': int(v(doc, 'related_count', 0)),
                'thumb_url': v(doc, 'thumb_url'),
                'title': v(doc, 'title'),
            })
        result['total_count'] = s.number_found
        cache_ttl = 3600
    else:
        # Default to returning suggestions.
        q = models.Content.query()
        q = q.filter(models.Content.tags == 'is suggestion')
        q = q.order(-models.Content.sort_index)
        content_list = q.fetch(20)
        creator_keys = {c.creator for c in content_list if c.creator.id() != config.ANONYMOUS_ID}
        creator_lookup = {a.key: a for a in ndb.get_multi(creator_keys)}
        for content in content_list:
            result['data'].append({
                'id': content.key.id(),
                'creator': creator_lookup.get(content.creator),
                'duration': content.duration,
                'original_url': content.original_url,
                'rank': content.search_rank,
                'related_count': content.related_count,
                'thumb_url': content.thumb_url,
                'title': content.title,
            })
        result['total_count'] = len(result['data'])
        cache_ttl = 300
    result_json = convert.to_json(result, **g.public_options)
    memcache.set(cache_key, result_json, time=cache_ttl)
    logging.debug('Saved to cache key %r (ttl: %d)', cache_key, cache_ttl)
    return convert.Raw(result_json)


@app.route('/<version>/content/<tags>/', methods=['GET'])
@flask_extras.json_service()
def get_content_tags(tags):
    # Parse and validate list of user specified tags separated by "+".
    tags = models.Content.parse_tags(tags, allow_restricted=True, separator='+')
    if not tags or any(models.Content.is_tag_unlisted(t) for t in tags):
        raise errors.UnsupportedEndpoint()
    sort = flask_extras.get_parameter('sort') or 'hot'
    # Add the "published" tag as a requirement for some tags.
    # Note that it's an unlisted tag so it cannot be specified by user.
    is_bot = 'ReactionCam/1337' in request.headers.get('User-Agent', '')
    if (tags == {'reaction'} or tags == {'reacttothis'}) and not is_bot:
        # Users can only see published #reaction and #reacttothis content.
        # Bots can see all content.
        tags.add('published')
    if tags == {'original'}:
        # Only expose some original content.
        if sort == 'hot':
            tags.add('is hot')
        else:
            tags.add('is reacted')
    elif tags == {'reaction', 'vlog'}:
        if sort == 'hot':
            tags.add('is hot')
    try:
        limit = int(flask_extras.get_parameter('limit') or 50)
        assert 0 < limit <= 500
    except:
        raise errors.InvalidArgument('Invalid limit')
    session = auth.get_session()
    session_key = session.account_key if session else None
    cursor_urlsafe = flask_extras.get_parameter('cursor')
    is_first_page = not cursor_urlsafe
    if is_first_page:
        # Attempt to get the data from cache (first page only).
        tags_string = '+'.join(sorted(tags))
        cache_key = 'content_%s_%s_%s_%d' % (g.api_version, tags_string, sort, limit)
        cache_json = memcache.get(cache_key)
    else:
        cache_json = None
    if cache_json:
        logging.debug('Loaded cache key %r', cache_key)
        return convert.Raw(_load_and_inject_votes(cache_json, session_key))
    # Read the data from datastore.
    q = models.Content.query()
    for tag in tags:
        q = q.filter(models.Content.tags == tag)
    if sort == 'hot':
        cache_ttl = 300
        q = q.order(-models.Content.sort_index)
    elif sort == 'recent':
        cache_ttl = 60
        q = q.order(-models.Content.created)
    elif sort == 'top':
        cache_ttl = 3600
        q = q.order(-models.Content.sort_bonus)
    else:
        raise errors.InvalidArgument('Invalid sort value')
    cursor = datastore_query.Cursor(urlsafe=cursor_urlsafe)
    content_future = q.fetch_page_async(limit, start_cursor=cursor)
    if is_first_page and tags == {'published', 'reaction'} and sort == 'recent':
        q2 = models.Content.query()
        q2 = q2.filter(models.Content.tags == 'featured')
        q2 = q2.order(-models.Content.created)
        extra_list = q2.fetch(limit // 3 + 1)
        content_list, next_cursor, more = content_future.get_result()
        content_keys = set(c.key for c in content_list)
        for i, extra in enumerate(e for e in extra_list if e.key not in content_keys):
            i = 2 + i * 4
            if i > len(content_list):
                break
            content_list[i:i] = [extra]
    else:
        content_list, next_cursor, more = content_future.get_result()
    # Hide some content.
    content_list = _filter_content(content_list, hide_flagged=('featured' not in tags and sort == 'recent'))
    # Look up extra data for the content list.
    lookup, votes = models.Content.decorate(content_list,
                                            include_creator=True,
                                            include_related=True,
                                            for_account_key=session_key)
    next_cursor_urlsafe = next_cursor.urlsafe() if more else None
    # Build the result and a payload to cache.
    cache_data = []
    data = []
    for i, content in enumerate(content_list):
        result = {
            'content': content,
            'creator': lookup[content.creator],
            'related_to': lookup.get(content.related_to),
            'voted': votes[i] is not None,
        }
        data.append(result)
        if is_first_page:
            # Create a marker that marks the content id so we can replace it with vote data.
            cache_marker = config.CONTENT_CACHE_MARKER + str(content.key.id())
            cache_data.append(dict(result, voted=cache_marker))
    if (tags == {'featured'} or tags == {'featured prank', 'reaction'}) and sort == 'hot':
        # Scramble featured content.
        if is_first_page:
            random.shuffle(cache_data)
        random.shuffle(data)
    if is_first_page:
        if g.api_version >= 45:
            cache_data = {'cursor': next_cursor_urlsafe, 'data': cache_data}
        else:
            cache_data = {'data': cache_data}
        cache_json = convert.to_json(cache_data, **g.public_options)
        memcache.set(cache_key, cache_json, time=cache_ttl)
        logging.debug('Saved to cache key %r (ttl: %d)', cache_key, cache_ttl)
    if g.api_version < 45:
        return {'data': data}
    return {'cursor': next_cursor_urlsafe, 'data': data}


@app.route('/<version>/device', methods=['GET'])
@flask_extras.json_service()
@auth.authed_request()
def get_device(session):
    """
    Return a list of registered devices.
    """
    return {
        'data': models.Device.query(ancestor=session.account_key).fetch()
    }


@app.route('/<version>/device', methods=['POST'])
@flask_extras.json_service()
@auth.authed_request()
def post_device(session):
    """
    Add a user's device token so that they can receive push notifications.

    Request:
        POST /v1/device?device_token=abcdef&platform=ios
        Authorization: Bearer 5ZVhC41gMAqjpno3ELKc5pLtcAbQAPO1bWJf6hCMFW8T20s

    Response:
        {"success": true}

    """
    client_id, _ = auth.get_client_details()
    if client_id == 'fika':
        app = 'io.fika.Fika'
    elif client_id == 'reactioncam':
        app = 'cam.reaction.ReactionCam'
    else:
        raise errors.InvalidArgument('Unsupported client')
    user_agent = request.headers.get('User-Agent', '')
    models.Device.add(session.account,
                      app=app,
                      device_id=flask_extras.get_parameter('device_id'),
                      device_info=user_agent,
                      environment=flask_extras.get_parameter('environment'),
                      platform=flask_extras.get_parameter('platform'),
                      token=flask_extras.get_parameter('device_token'),
                      api_version=g.api_version)
    return {'success': True}


@app.route('/<version>/device/<device_token>', methods=['PUT'])
@flask_extras.json_service()
@auth.authed_request(update_last_active=False)
def put_device(session, device_token):
    client_id, _ = auth.get_client_details()
    if client_id == 'fika':
        app = 'io.fika.Fika'
    elif client_id == 'reactioncam':
        app = 'cam.reaction.ReactionCam'
    else:
        raise errors.InvalidArgument('Unsupported client')
    user_agent = request.headers.get('User-Agent', '')
    device = models.Device.add(
        session.account,
        app=app,
        device_id=flask_extras.get_parameter('device_id'),
        device_info=user_agent,
        environment=flask_extras.get_parameter('environment'),
        platform=flask_extras.get_parameter('platform'),
        token=device_token,
        api_version=g.api_version)
    return device


@app.route('/<version>/device/<token>', methods=['DELETE'])
@flask_extras.json_service()
@auth.authed_request()
def delete_device_token(session, token):
    """
    Remove a user's device token.

    Request:
        DELETE /v1/device/abcdef
        Authorization: Bearer 5ZVhC41gMAqjpno3ELKc5pLtcAbQAPO1bWJf6hCMFW8T20s

    Response:
        {"success": true}

    """
    models.Device.delete(session.account, token)
    return {'success': True}


@app.route('/<version>/email/', methods=['POST'])
@flask_extras.json_service()
@auth.authed_request()
def post_email(session):
    # Admin only endpoint.
    if not session.account.admin:
        raise errors.UnsupportedEndpoint()
    data = json.loads(flask_extras.get_parameter('data'))
    message = mail.EmailMessage(sender=data['sender'])
    for item in data['items']:
        message.to = item['to']
        message.subject = data['subject'].format(**item['data'])
        message.body = data['text'].format(**item['data'])
        message.html = data['html'].format(**item['data'])
        message.check_initialized()
        message.send()
    return {'success': True}


@app.route('/<version>/event', methods=['POST'])
@flask_extras.json_service()
@auth.authed_request(update_last_active=False)
def post_event(session):
    name = flask_extras.get_parameter('name')
    if not name:
        raise errors.InvalidArgument('Invalid name')
    properties = flask_extras.get_json_properties('properties') or {}
    params = {'properties': properties}
    event_class = flask_extras.get_parameter('class')
    if event_class:
        params['event_class'] = event_class
    models.AccountEvent.create(session.account_key, name, **params)
    return {'success': True}


@app.route('/<version>/event/<account_id>/batch', methods=['POST'])
@flask_extras.json_service()
@auth.authed_request(update_last_active=False)
def post_event_account_id_batch(session, account_id):
    try:
        account_id = int(account_id)
        assert account_id == session.account_id
    except:
        raise errors.UnsupportedEndpoint()
    items = []
    data = request.data.strip()
    if not data:
        return {'success': True}
    lines = data.split('\n')
    for line in lines:
        values = line.split('\t')
        try:
            assert len(values) == 4
            timestamp = convert.from_unix_timestamp_ms(int(values[0]))
            event_name = values[1]
            event_class = values[2]
            properties = json.loads(values[3])
        except:
            logging.warning('Erroneous line %r', line)
            continue
        item = models.AccountEventItem(timestamp, event_name, event_class=event_class,
                                       properties=properties)
        # Update previous item instead if the new one is a repetition of it.
        prev = items[-1] if items else None
        if prev and prev.is_repeat_of(item):
            prev.increment_repeats(timestamp)
            continue
        items.append(item)
    if not items:
        return {'success': True}
    try:
        models.AccountEvent.create_batch(session.account_key, items)
    except:
        logging.exception('Dropped %d items', len(items))
    return {'success': True}


@app.route('/<version>/event/<account_id>/<int:event_id>', methods=['PUT'])
@flask_extras.json_service()
@auth.authed_request()
def put_event_account_id_event_id(session, account_id, event_id):
    try:
        account_id = int(account_id)
        assert account_id == session.account_id
        event_id = int(event_id)
        timestamp = convert.from_unix_timestamp_ms(event_id)
    except:
        raise errors.UnsupportedEndpoint()
    name = flask_extras.get_parameter('name')
    if not name:
        raise errors.InvalidArgument('Invalid name')
    properties = flask_extras.get_json_properties('properties') or {}
    params = {'properties': properties}
    event_class = flask_extras.get_parameter('class')
    if event_class:
        params['event_class'] = event_class
    if name == 'Presentation':
        # Just ignore this troublesome little devil.
        logging.debug('Ignored %s (%s):\n%r', name, timestamp, params)
        return {'success': True}
    try:
        models.AccountEvent.create(session.account_key, name, timestamp=timestamp, **params)
    except:
        logging.exception('Dropped %s (%s):\n%r', name, timestamp, params)
    return {'success': True}


@app.route('/<version>/facebook/auth', methods=['POST'])
@flask_extras.json_service()
def post_facebook_auth():
    client_id, _ = auth.get_client_details()
    access_token = flask_extras.get_parameter('access_token')
    if not access_token:
        raise errors.MissingArgument('A Facebook token is required')
    try:
        url = ('https://graph.facebook.com/v2.10/me?access_token=%s'
               '&fields=id,email,name,picture.type(large){url}')
        result = urlfetch.fetch(method=urlfetch.GET, url=url % (access_token,))
        data = json.loads(result.content)
        try:
            identifier = 'facebook:%s' % (data['id'],)
        except KeyError:
            logging.error('Unexpected data (HTTP %d): %r', result.status_code, data)
            raise
    except Exception as e:
        logging.exception('Fetching Facebook profile failed.')
        raise errors.InvalidCredentials('Invalid Facebook access token')
    session = auth.get_session()
    if session:
        handler = accounts.get_handler(session.account)
        handler.add_identifier(identifier)
    else:
        handler = accounts.get_or_create(identifier,
            image=data['picture']['data']['url'] if 'picture' in data else None)
        session = handler.create_session()
        taskqueue.add(url='/_ah/jobs/track_login',
                      countdown=20,
                      params={'account_id': handler.account_id,
                              'client_id': client_id,
                              'fb_token': access_token},
                      queue_name=config.INTERNAL_QUEUE)
    email = data.get('email')
    if email:
        try:
            handler.add_identifier(email)
        except:
            logging.exception('Could not add Facebook email:')
    g.public_options['include_extras'] = True
    g.public_options['view_account'] = session.account
    return session


@app.route('/<version>/featured', methods=['GET'])
@flask_extras.json_service()
def get_featured():
    featured = streams.get_featured()
    return {
        'streams': featured,
    }


@app.route('/<version>/feedback', methods=['POST'])
@flask_extras.json_service()
def post_feedback():
    message = flask_extras.get_parameter('message')
    if not message:
        raise errors.InvalidArgument('Missing message parameter')
    email = flask_extras.get_parameter('email')
    session = auth.get_session()
    if session:
        models.AccountEvent.create(session.account_key, 'Feedback', event_class='info',
                                   properties={'email': email, 'message': message})
    subject = message.replace('\n', ' ')
    if len(subject) > 200:
        subject = u'%s…' % (subject[:200],)
    client_id, _ = auth.get_client_details()
    def send_email(sender):
        localize.send_email(client_id, 'feedback',
            to='yo@reaction.cam',
            to_name='team reaction.cam',
            sender=sender,
            sender_name=session.account.display_name if session else None,
            cc_sender=False,
            subject=subject,
            email=email,
            identifier=str(session.account_id) if session else (email or ''),
            message=message,
            message_html=cgi.escape(message).replace('\n', '<br>'),
            user_agent=request.headers.get('User-Agent', 'unknown'))
    try:
        send_email(email)
    except:
        logging.warning('Reattempting email without user entered email')
        send_email(None)
    return {'success': True}


@app.route('/<version>/ifttt', methods=['POST'])
@flask_extras.json_service()
@auth.authed_request()
def post_ifttt(session):
    """Ping IFTTT to refetch data."""
    utils.ping_ifttt(session.account_key)
    return {'success': True}


@app.route('/<version>/invite', methods=['POST'])
@flask_extras.json_service()
@auth.authed_request()
def post_invite(session):
    identifier_list = flask_extras.get_parameter_list('identifier')
    name_list = flask_extras.get_parameter_list('name')
    if not name_list:
        name_list = [None] * len(identifier_list)
    invite_token = flask_extras.get_parameter('invite_token')
    client_id, _ = auth.get_client_details()
    account_map = {}
    for identifier, name in zip(identifier_list, name_list):
        identifier, identifier_type = identifiers.parse(identifier)
        cache_key = 'request_invite_link:{}'.format(zlib.adler32(identifier))
        if memcache.get(cache_key):
            logging.warning('An invite was ignored because it is being throttled')
        if identifier_type == identifiers.EMAIL:
            invited = accounts.get_or_create(identifier, display_name=name, status='invited')
            account_map[identifier] = {
                'id': invited.account_id,
                'active': invited.is_active,
            }
            # Set up email template values.
            params = dict(
                invited_name=invited.display_name,
                inviter_name=session.account.display_name)
            # Find an email in the inviter's identifiers.
            for i in session.account.identifiers:
                _, it = identifiers.parse(i.id())
                if it != identifiers.EMAIL:
                    continue
                params.update(sender=identifiers.email(i.id()))
                break
            localize.send_email(client_id, 'invite',
                to=identifiers.email(identifier),
                to_name=invited.display_name,
                sender_name=session.account.display_name,
                **params)
            streams.get_or_create(session.account, [invited.account])
        elif identifier_type == identifiers.PHONE:
            # SMS invites.
            identity = models.Identity.get_by_id(identifier)
            if identity and identity.account:
                key, active = identity.account, identity.is_active
            else:
                invited = accounts.create(identifier, display_name=name, status='invited')
                key, active = invited.key, invited.is_active
            account_map[identifier] = {
                'id': key.id(),
                'active': active,
            }
            if active:
                # This account doesn't need an invite SMS.
                continue
            if name:
                name = name.split(' ')[0]
                external.send_sms(identifier, 'invite_personal', args={
                    'to': name,
                    'from': session.account.display_name,
                    'others': random.randint(3, 7),
                    'link': 'TODO',
                })
            else:
                external.send_sms(identifier, 'invite', args={
                    'name': session.account.display_name,
                    'others': random.randint(3, 7),
                    'link': 'TODO',
                })
            report.invite(session.account_id, key.id(), view='quick-invite')
        else:
            logging.warning('Unhandled identifier %s', identifier)
        memcache.set(cache_key, True, config.THROTTLE_INVITE)
    return {'map': account_map}


@app.route('/<version>/notify/', methods=['POST'])
@flask_extras.json_service()
@auth.authed_request()
def post_notify(session):
    # Admin only endpoint.
    if not session.account.admin:
        raise errors.UnsupportedEndpoint()
    identifier_list = flask_extras.get_parameter_list('identifier')
    account_keys = models.Account.resolve_keys(identifier_list)
    if not account_keys or not all(account_keys):
        raise errors.ResourceNotFound('That account does not exist')
    notif_data = json.loads(flask_extras.get_parameter('data'))
    if not notif_data.get('title') or not notif_data.get('text'):
        raise errors.InvalidArgument('Invalid title/text')
    futures = []
    for account_key in account_keys:
        hub = notifs.Hub(account_key)
        f = hub.emit_async('custom', **notif_data)
        futures.append(f)
    _wait_all(futures)
    return {'success': True}


@app.route('/<version>/original', methods=['PUT'])
@flask_extras.json_service()
def put_original():
    content_key, content = utils.get_or_create_content_from_request(None)
    if not content and content_key:
        content = content_key.get()
    session = auth.get_session()
    if not content or (session and not content.visible_by(session.account.key)):
        raise errors.ResourceNotFound('Content not found')
    if session and content.creator in session.account.blocked_by:
        raise errors.ResourceNotFound('Content not found')
    creator, related_to, voted = content.decoration_info(
        include_creator=True,
        include_related=True,
        for_account_key=session.account.key if session else None)
    return {
        'content': content,
        'creator': creator,
        'related_to': related_to,
        'voted': voted,
    }


@app.route('/<version>/original/', methods=['GET'])
@flask_extras.json_service()
def get_original():
    sort = flask_extras.get_parameter('sort') or 'hot'
    try:
        limit = int(flask_extras.get_parameter('limit') or 50)
        assert 0 < limit <= 500
    except:
        raise errors.InvalidArgument('Invalid limit')
    cursor_urlsafe = flask_extras.get_parameter('cursor')
    # Attempt to get the data from cache.
    cache_key = 'original_%s_%s_%d_%s' % (g.api_version, sort, limit, cursor_urlsafe)
    cache_json = memcache.get(cache_key)
    if cache_json:
        logging.debug('Loaded cache key %r', cache_key)
        return convert.Raw(cache_json)
    # Query the data from the datastore.
    q = models.Content.query()
    q = q.filter(models.Content.tags == 'original')
    if sort == 'hot':
        q = q.filter(models.Content.tags == 'is hot')
        q = q.order(-models.Content.sort_index)
        cache_ttl = 600
    elif sort == 'recent':
        q = q.filter(models.Content.tags == 'is reacted')
        q = q.order(-models.Content.created)
        cache_ttl = 60 if not cursor_urlsafe else 600
    elif sort == 'top':
        q = q.order(-models.Content.sort_bonus)
        cache_ttl = 86400
    else:
        raise errors.InvalidArgument('Invalid sort value')
    cursor = datastore_query.Cursor(urlsafe=cursor_urlsafe)
    content_list, next_cursor, more = q.fetch_page(limit, start_cursor=cursor)
    # Look up extra data for the content list.
    related_futures = []
    for content in content_list:
        q = models.Content.query()
        q = q.filter(models.Content.related_to == content.key)
        q = q.filter(models.Content.tags == 'reaction')
        q = q.order(-models.Content.sort_bonus)
        related_futures.append(q.fetch_async(10))
    creator_keys = set(c.creator for c in content_list if c.creator.id() != config.ANONYMOUS_ID)
    poster_keys = set(c.first_related_creator for c in content_list if c.first_related_creator)
    poster_keys.add(ndb.Key('Account', config.REACTION_CAM_ID))
    account_lookup = {a.key: a for a in ndb.get_multi(creator_keys | poster_keys) if a}
    # Build the result and a payload to cache.
    result = {'cursor': next_cursor.urlsafe() if more else None, 'data': []}
    for i, content in enumerate(content_list):
        related_source = related_futures[i].get_result()
        related = []
        for r in related_source:
            if 'repost' in r.tags:
                continue
            if any(r.creator == rr.creator for rr in related):
                continue
            related.append(r)
            if len(related) >= 5:
                break
        poster_key = content.first_related_creator or ndb.Key('Account', config.REACTION_CAM_ID)
        result['data'].append({
            'content': content,
            'creator': account_lookup.get(content.creator),
            'poster': account_lookup.get(poster_key),
            'related': related,
        })
    cache_json = convert.to_json(result, **g.public_options)
    memcache.set(cache_key, cache_json, time=cache_ttl)
    logging.debug('Saved to cache key %r (ttl: %d)', cache_key, cache_ttl)
    return result


@app.route('/<version>/pay/feed/', methods=['GET'])
@flask_extras.json_service()
def get_pay_feed():
    cache_key = 'pay_feed_%s' % (g.api_version,)
    cache_json = memcache.get(cache_key)
    if cache_json:
        logging.debug('Loaded cache key %r', cache_key)
        return convert.Raw(cache_json)
    # TODO: Improve and index this query to fetch less duds.
    q = models.WalletTransaction.query()
    q = q.order(-models.WalletTransaction.timestamp)
    txs = q.fetch(1000)
    # Filter out transactions where coins were removed.
    txs = filter(lambda tx: tx.delta > 0, txs)
    # Filter out transactions where user bought coins.
    txs = filter(lambda tx: tx.receiver != tx.sender, txs)
    # Don't show free coins via admin (sent by @reaction.cam).
    txs = filter(lambda tx: tx.sender != ndb.Key('Account', config.REACTION_CAM_ID), txs)
    # Get all accounts decorated.
    keys = set(k for tx in txs for k in [tx.receiver, tx.sender])
    lookup = {a.key: a for a in ndb.get_multi(keys)}
    # Create data structure for feed.
    data = []
    for tx in txs:
        comment = re.sub(r'^Payment \((.*?)\)$', r'\1', tx.comment)
        if comment == 'None':
            comment = None
        if not comment and convert.unix_timestamp_ms(tx.timestamp) % 1000 < 950:
            # Skip ~95% of bot transactions.
            continue
        data.append({
            'amount': tx.delta,
            'comment': comment,
            'receiver': lookup[tx.receiver],
            'sender': lookup[tx.sender],
            'timestamp': tx.timestamp,
        })
    result = {'data': data[:20]}
    cache_json = convert.to_json(result, **g.public_options)
    cache_ttl = 300
    memcache.set(cache_key, cache_json, time=cache_ttl)
    logging.debug('Saved to cache key %r (ttl: %d)', cache_key, cache_ttl)
    return result


@app.route('/<version>/pay/toplist/', methods=['GET'])
@flask_extras.json_service()
def get_pay_toplist():
    cache_key = 'pay_toplist_%s' % (g.api_version,)
    cache_json = memcache.get(cache_key)
    if cache_json:
        logging.debug('Loaded cache key %r', cache_key)
        return convert.Raw(cache_json)
    account_keys = [
        ndb.Key('Account', 6431912322138112),  # @okaytinashe
        ndb.Key('Account', 5887536569253888),  # @broitstreyneal
        ndb.Key('Account', 5662506279239680),  # @xavierthelegend
        ndb.Key('Account', 6535205648072704),  # @bossreacts
        ndb.Key('Account', 6606478627569664),  # @dguwopp
        ndb.Key('Account', 5244280189747200),  # @mysterioushatter
        ndb.Key('Account', 4627818473324544),  # @hakeemprimereactions
        ndb.Key('Account', 6088998832308224),  # @lydvidtvvlogz
    ]
    accounts = ndb.get_multi(account_keys)
    data = []
    for i, account in enumerate(accounts):
        if not account or 'tiers' not in account.properties:
            logging.warning('Cannot create rewards for %d', account_keys[i].id())
            continue
        for tier in account.properties['tiers']:
            data.append({
                'account_id': account.key.id(),
                'coins': tier['coins'],
                'description': tier['description'],
                'image_url': account.image_url,
                'title': tier['title'],
                'username': account.username,
            })
    random.shuffle(data)
    result = {'data': data[:10]}
    cache_json = convert.to_json(result, **g.public_options)
    cache_ttl = 3600
    memcache.set(cache_key, cache_json, time=cache_ttl)
    logging.debug('Saved to cache key %r (ttl: %d)', cache_key, cache_ttl)
    return result


@app.route('/<version>/profile/', methods=['GET'])
@flask_extras.json_service()
def get_profile():
    session = auth.get_session()
    if session:
        g.public_options['view_account'] = session.account
    g.public_options['include_extras'] = True
    identifier_list = flask_extras.get_parameter_list('identifier')
    try:
        accounts = models.Account.resolve_list(identifier_list)
        for account in accounts:
            # Make sure none of the accounts are deleted/banned or have blocked the user.
            assert account.can_make_requests
            if session:
                assert account.key not in session.account.blocked_by
        return {'data': accounts}
    except:
        raise errors.ResourceNotFound('One or more accounts did not exist')


@app.route('/<version>/profile/me', methods=['GET'])
@flask_extras.json_service()
@auth.authed_request(allow_nonactive=True, set_view_account=True, update_last_active=False)
def get_profile_me(session):
    """
    Gets the current user’s profile.

    Request:
        GET /v2/profile/me
        Authorization: Bearer 5ZVhC41gMAqjpno3ELKc5pLtcAbQAPO1bWJf6hCMFW8T20s

    Response:
        {
            "id": 12345678,
            "display_name": "Bob Brown",
            "aliases": ["bob", "bob@example.com"],
            "image_url": null
        }

    """
    g.public_options['include_extras'] = True
    return session.account


@app.route('/<version>/profile/me', methods=['POST'])
@flask_extras.json_service()
@auth.authed_request(set_view_account=True)
def post_profile_me(session):
    """
    Updates the current user's profile.

    Request:
        POST /v2/profile/me?display_name=Bob&username=yolo
        Authorization: Bearer 5ZVhC41gMAqjpno3ELKc5pLtcAbQAPO1bWJf6hCMFW8T20s

    Response:
        {
            "id": 12345678,
            "display_name": "Bob",
            "aliases": ["yolo", "bob@example.com"],
            "image_url": null
        }

    """
    handler = accounts.get_handler(session.account)
    new_password = flask_extras.get_parameter('new_password')
    if new_password:
        if handler.has_password:
            old_password = flask_extras.get_parameter('password')
            if not old_password:
                raise errors.MissingArgument('Your current password must be specified')
            if not handler.validate_password(old_password):
                raise errors.InvalidArgument('Incorrect password')
        # TODO: Invalidate old sessions.
        handler.set_password(new_password)
    # Update username.
    new_username = flask_extras.get_parameter('username')
    if new_username:
        handler.set_username(new_username)
    birthday = flask_extras.get_parameter('birthday')
    gender = flask_extras.get_parameter('gender')
    if birthday is not None or gender is not None:
        handler.update_demographics(birthday, gender)
    # Update display name.
    display_name = flask_extras.get_parameter('display_name')
    if display_name is not None:
        handler.set_display_name(display_name)
    image = request.files.get('image')
    if image:
        path = files.upload(image.filename, image.stream, persist=True)
        # TODO this triggers another write to the model, conflate later
        handler.set_image(path)
    location = flask_extras.get_parameter('location')
    if location:
        try:
            point = ndb.GeoPt(location)
        except:
            raise errors.InvalidArgument('Invalid location (expected lat,lng)')
        handler.account.set_location(point=point, defer=False)
    share_location = flask_extras.get_flag('share_location')
    if share_location is not None:
        handler.account.share_location = share_location
        handler.account.put()
    if not handler.account.properties:
        handler.account.properties = {}
    properties = flask_extras.get_json_properties(
        'properties',
        apply_to_dict=handler.account.properties)
    if properties:
        for key in properties:
            if key in config.PREMIUM_PROPERTIES and key not in handler.premium_properties:
                raise errors.ForbiddenAction('Cannot set "%s" before unlocking it' % (key,))
        handler.account.put()
    g.public_options['include_extras'] = True
    return handler.account


@app.route('/<version>/profile/me/unlock', methods=['POST', 'PUT'])
@flask_extras.json_service()
@auth.authed_request()
def post_profile_me_unlock(session):
    prop_name = flask_extras.get_parameter('property')
    if not prop_name:
        raise errors.InvalidArgument('No property to unlock was provided')
    if prop_name not in config.PREMIUM_PROPERTIES:
        raise errors.InvalidArgument('That is not an unlockable property')
    if prop_name in session.account.premium_properties:
        # Already unlocked; no-op.
        return {'success': True}
    if not session.account.wallet:
        raise errors.ServerError('Account does not have a wallet')
    cost = config.PREMIUM_PROPERTIES[prop_name]
    dest_wallet_key = models.Wallet.key_from_id(config.PURCHASES_WALLET_ID)
    tx = models.Wallet.create_tx(
        session.account.key, session.account.wallet,
        dest_wallet_key, cost, 'Unlock %s' % (prop_name,))
    @ndb.transactional(xg=True)
    def unlock():
        account_future = session.account_key.get_async()
        tx_future = tx()
        account = account_future.get_result()
        if prop_name in account.premium_properties:
            # Property was unlocked in another transaction, roll back this one.
            raise ndb.Rollback()
        account.premium_properties.append(prop_name)
        account.put()
        # Ensure that transaction is successful before leaving transaction.
        tx_future.get_result()
    unlock()
    return {'success': True}


@app.route('/<version>/profile/me/blocked', methods=['POST'])
@flask_extras.json_service()
@auth.authed_request()
def post_profile_me_blocked(session):
    """
    Updates the current user's blocked list.

    Request:
        POST /v2/profile/me/blocked?identifier=djmacradio
        Authorization: Bearer 5ZVhC41gMAqjpno3ELKc5pLtcAbQAPO1bWJf6hCMFW8T20s

    Response:
        {
            "success": true
        }

    """
    identifier = flask_extras.get_parameter('identifier')
    account = models.Account.resolve(identifier)
    if not account:
        raise errors.ResourceNotFound('That account does not exist')
    handler = accounts.get_handler(session.account)
    handler.block(account)
    slack_api.message(channel='#abuse', hook_id='reactioncam', text='{} blocked {}'.format(
        slack_api.admin(session.account),
        slack_api.admin(account)))
    return {'success': True}


@app.route('/<version>/profile/me/blocked/<identifier>', methods=['DELETE'])
@flask_extras.json_service()
@auth.authed_request()
def delete_profile_me_blocked(session, identifier):
    """
    Updates the current user's blocked list.

    Request:
        POST /v2/profile/me/blocked/djmacradio
        Authorization: Bearer 5ZVhC41gMAqjpno3ELKc5pLtcAbQAPO1bWJf6hCMFW8T20s

    Response:
        {
            "success": true
        }

    """
    handler = accounts.get_handler(session.account)
    handler.unblock(identifier)
    return {'success': True}


@app.route('/<version>/profile/me/chat/announce', methods=['POST'])
@flask_extras.json_service()
@auth.authed_request()
def post_profile_me_chat_announce(session):
    text = flask_extras.get_parameter('text')
    if not text:
        raise errors.InvalidArgument('Missing text')
    taskqueue.add(url='/_ah/jobs/chat_announce',
                  params={'owner_id': session.account_id,
                          'channel_id': str(session.account_id),
                          'text': text},
                  queue_name=config.INTERNAL_QUEUE,
                  retry_options=taskqueue.TaskRetryOptions(task_retry_limit=0))
    return {'success': True}


@app.route('/<version>/profile/me/content/<tag>/', methods=['GET'])
@flask_extras.json_service()
@auth.authed_request()
def get_profile_me_content_tag(session, tag):
    if models.Content.is_tag_unlisted(tag):
        raise errors.UnsupportedEndpoint()
    try:
        limit = int(flask_extras.get_parameter('limit') or 50)
        assert 0 < limit <= 500
    except:
        raise errors.InvalidArgument('Invalid limit')
    q = models.Content.query()
    q = q.filter(models.Content.creator == session.account_key)
    q = q.filter(models.Content.tags == tag)
    q = q.order(-models.Content.created)
    cursor_urlsafe = flask_extras.get_parameter('cursor')
    cursor = datastore_query.Cursor(urlsafe=cursor_urlsafe)
    #content_list, next_cursor, more = q.fetch_page(limit, start_cursor=cursor)
    content_list, next_cursor, more = [], None, False
    lookup, votes = models.Content.decorate(content_list,
        include_related=True,
        for_account_key=session.account_key)
    # Build the result.
    data = []
    for i, content in enumerate(content_list):
        result = {
            'content': content,
            'creator': session.account,
            'related_to': lookup.get(content.related_to),
            'voted': votes[i] is not None,
        }
        data.append(result)
    return {'cursor': next_cursor.urlsafe() if more else None, 'data': data}


@app.route('/<version>/profile/me/followers/', methods=['GET'])
@flask_extras.json_service()
@auth.authed_request(set_view_account=True)
def get_profile_me_followers(session):
    try:
        limit = int(flask_extras.get_parameter('limit') or 50)
    except:
        raise errors.InvalidArgument('Invalid limit')
    ids_only = flask_extras.get_flag('ids_only') or False
    start_cursor = datastore_query.Cursor(urlsafe=flask_extras.get_parameter('cursor'))
    results, next_cursor, more = models.AccountFollow.fetch_followers_page(
        session.account_key, limit, keys_only=ids_only, start_cursor=start_cursor)
    if ids_only:
        results = map(lambda k: k.id(), results)
    else:
        g.public_options['include_extras'] = True
    return {'cursor': next_cursor.urlsafe() if more else None, 'data': results}


@app.route('/<version>/profile/me/following/', methods=['GET'])
@flask_extras.json_service()
@auth.authed_request(set_view_account=True, update_last_active=False)
def get_profile_me_following(session):
    try:
        limit = int(flask_extras.get_parameter('limit') or 50)
    except:
        raise errors.InvalidArgument('Invalid limit')
    ids_only = flask_extras.get_flag('ids_only') or False
    start_cursor = datastore_query.Cursor(urlsafe=flask_extras.get_parameter('cursor'))
    results, next_cursor, more = models.AccountFollow.fetch_following_page(
        session.account_key, limit, keys_only=ids_only, start_cursor=start_cursor)
    if ids_only:
        results = map(lambda k: k.id(), results)
    else:
        g.public_options['include_extras'] = True
    return {'cursor': next_cursor.urlsafe() if more else None, 'data': results}


@app.route('/<version>/profile/me/following/<identifier>', methods=['DELETE'])
@flask_extras.json_service()
@auth.authed_request()
def delete_profile_me_following_identifier(session, identifier):
    account_key = models.Account.resolve_key(identifier)
    if not account_key:
        raise errors.ResourceNotFound('That account does not exist')
    future = models.AccountFollow.unfollow_async(session.account_key, account_key)
    a, b = future.get_result()
    if not b:
        logging.debug('Request did not result in an unfollow')
    return {'success': True}


@app.route('/<version>/profile/me/following/<identifiers>', methods=['PUT'])
@flask_extras.json_service()
@auth.authed_request()
def put_profile_me_following_identifiers(session, identifiers):
    try:
        account_keys = models.Account.resolve_keys(identifiers.split(','))
    except ValueError:
        raise errors.ResourceNotFound('That account does not exist')
    if not account_keys:
        raise errors.ResourceNotFound('That account does not exist')
    if session.account_key in account_keys:
        raise errors.InvalidArgument('Cannot follow current account')
    if any(k in session.account.blocked_by for k in account_keys):
        raise errors.ResourceNotFound('That account does not exist')
    future = models.AccountFollow.follow_async(session.account_key, account_keys)
    a, b_list = future.get_result()
    if b_list:
        hubs = [accounts.get_handler(b).notifs for b in b_list]
        futures = [h.emit_async(notifs.ON_ACCOUNT_FOLLOW, follower=a) for h in hubs]
        _wait_all(futures)
    else:
        logging.debug('Request did not result in a follow')
        return {'success': True}
    if not a.wallet or not a.wallet_bonus:
        logging.warning('No wallets available to award bonus')
        return {'success': True}
    coins = len(b_list)
    try:
        future = models.Wallet.create_tx_async(a.key, a.wallet_bonus, a.wallet, coins,
                                               u'Bonus', require_full_amount=False)
        tx = future.get_result()
        w1, _, w2, tx2 = tx().get_result()
        logging.debug('Gave bonus of %d to %d', tx2.delta, a.key.id())
    except models.WalletInsufficientFunds:
        logging.debug('No bonus left for %d', a.key.id())
    except:
        logging.exception('Failed to award bonus.')
    return {'success': True}


@app.route('/<version>/profile/me/following/content/<tag>/', methods=['GET'])
@flask_extras.json_service()
@auth.authed_request()
def get_profile_me_following_content_tag(session, tag):
    if models.Content.is_tag_unlisted(tag):
        raise errors.UnsupportedEndpoint()
    try:
        limit = int(flask_extras.get_parameter('limit') or 50)
        assert 0 < limit <= 500
    except:
        raise errors.InvalidArgument('Invalid limit')
    # Get all the individual feeds of accounts followed by session account.
    session_future = _get_single_feed_async(session.account_key, tag)
    account_keys, _, _ = models.AccountFollow.fetch_following_page(session.account_key,
                                                                   100, keys_only=True)
    futures = [session_future] + [_get_single_feed_async(k, tag) for k in account_keys]
    _wait_all(futures)
    futures_and_feeds = [f.get_result() for f in futures]
    futures, feeds = zip(*futures_and_feeds)
    # Weave the feeds together into one feed.
    content_list = sorted(itertools.chain.from_iterable(feeds),
                          key=lambda c: c.created, reverse=True)
    # Restrict the content list to the limit, then decorate it.
    logging.debug('Fetched %d Content entities for feed of max %d', len(content_list), limit)
    content_list = content_list[:limit]
    lookup, votes = models.Content.decorate(content_list,
        include_related=True,
        for_account_key=session.account_key)
    # Add in the creators in the lookup.
    for future in futures:
        account = future.get_result()
        lookup[account.key] = account
    # Build the final result.
    data = []
    for i, content in enumerate(content_list):
        result = {
            'content': content,
            'creator': lookup[content.creator],
            'related_to': lookup.get(content.related_to),
            'voted': votes[i] is not None,
        }
        data.append(result)
    return {'data': data}


@app.route('/<version>/profile/me/notifications/', methods=['GET'])
@flask_extras.json_service()
@auth.authed_request(update_last_active=False)
def get_profile_me_notifications(session):
    q = models.AccountNotification.recent_query(session.account_key)
    notifs = q.fetch(50)
    # Inject a fake update notif if user is on an old version.
    match = re.match(r'(\w+)/(\d+)', request.headers.get('User-Agent', ''))
    try:
        client, build = match.group(1), int(match.group(2))
        min_build = config.MINIMUM_BUILD[client]
    except:
        logging.warning('Could not check build number')
        client, build = None, None
        min_build = None
    if g.api_version >= 47 and build < min_build:
        notif = models.AccountNotification(
            key=ndb.Key('AccountNotification', -1, parent=session.account_key),
            properties={},
            timestamp=datetime.utcnow(),
            type='update-app')
        notifs.insert(0, notif)
    return {
        'data': notifs,
    }


@app.route('/<version>/profile/me/notifications/<notif_id>', methods=['POST'])
@flask_extras.json_service()
@auth.authed_request()
def post_profile_me_notifications_notif_id(session, notif_id):
    try:
        notif_id = int(notif_id)
    except:
        raise errors.InvalidArgument('Invalid notification id')
    seen = flask_extras.get_flag('seen')
    if not seen:
        raise errors.InvalidArgument('Only supported parameter is seen=true')
    notif = models.AccountNotification.get_by_id(notif_id, parent=session.account_key)
    if not notif:
        raise errors.ResourceNotFound()
    if not notif.seen:
        notif.seen = True
        notif.seen_timestamp = datetime.utcnow()
        notif.put()
    return {'success': True}


@app.route('/<version>/profile/me/requests/', methods=['POST'])
@flask_extras.json_service()
@auth.authed_request()
def post_profile_me_requests(session):
    try:
        delay = int(flask_extras.get_parameter('delay'))
        assert delay > 0
    except:
        raise errors.InvalidArgument('Specify a delay greater than zero')
    content_key, content = utils.get_or_create_content_from_request()
    if not content_key:
        raise errors.ResourceNotFound('That content does not exist')
    if not content:
        content = content_key.get()
    if not content or not content.is_public:
        raise errors.ResourceNotFound('That content does not exist')
    taskqueue.add(url='/_ah/jobs/content_request_send',
                  countdown=delay,
                  params={'requester_id': config.REACTION_CAM_ID,
                          'account_id': session.account_id,
                          'content_id': content.key.id()},
                  queue_name=config.INTERNAL_QUEUE)
    return {'success': True}


@app.route('/<version>/profile/search', methods=['GET'])
@flask_extras.json_service()
def get_profile_search():
    query = flask_extras.get_parameter('query') or ''
    query = re.sub(r'[^a-z0-9._-]+', '', query.lower())[:20]
    cache_key = 'profile_search_%s_%s' % (g.api_version, base64.b64encode(query.encode('utf-8')))
    result_json = memcache.get(cache_key)
    if result_json:
        logging.debug('Loaded cache key %r', cache_key)
        return convert.Raw(result_json)
    top_index_future = _get_top_accounts_index_async()
    account_keys = []
    if query:
        query_end = query + u'\ufffd'
        def exclude_prefix(q, prefix):
            if query <= prefix < query_end:
                q = q.filter(models.Identity.key < ndb.Key('Identity', prefix))
                q = q.filter(models.Identity.key > ndb.Key('Identity', prefix + '\ufffd'))
            return q
        q = models.Identity.query()
        q = q.filter(models.Identity.key >= ndb.Key('Identity', query))
        q = exclude_prefix(q, u'email:')
        q = exclude_prefix(q, u'facebook:')
        q = exclude_prefix(q, u'slack:')
        q = exclude_prefix(q, u'youtube:')
        q = q.filter(models.Identity.key < ndb.Key('Identity', query_end))
        identities = q.fetch(300)
        top_index = top_index_future.get_result()
        for entry in top_index:
            if difflib.SequenceMatcher(None, query, entry[0]).ratio() < 0.7:
                continue
            account_key = ndb.Key('Account', entry[1])
            if account_key in account_keys:
                continue
            account_keys.append(account_key)
            if len(account_keys) >= 20:
                break
        for identity in identities:
            if not identity.account or identity.account in account_keys:
                continue
            account_keys.append(identity.account)
    else:
        # Just return top followed users for empty search queries.
        top_index = top_index_future.get_result()
        for entry in top_index:
            account_keys.append(ndb.Key('Account', entry[1]))
    cut_off_date = date.today() - timedelta(days=90)
    def account_filter(a):
        if not a.can_make_requests:
            return False
        if a.username == query:
            return True
        if a.status == 'unclaimed' or a.verified:
            return True
        return a.last_active >= cut_off_date
    def account_sort_key(a):
        if a.username == query:
            return 1000000
        if a.status == 'unclaimed' or a.verified:
            days_since_active = 0  # These accounts are important even if inactive.
        else:
            days_since_active = (date.today() - a.last_active).days
        return a.follower_count // 10 - days_since_active * 2 + a.quality * 25
    accounts = ndb.get_multi(account_keys)
    accounts = filter(account_filter, accounts)
    accounts.sort(key=account_sort_key, reverse=True)
    result = {'data': accounts[:20]}
    result_json = convert.to_json(result, **g.public_options)
    cache_ttl = 3600
    memcache.set(cache_key, result_json, time=cache_ttl)
    logging.debug('Saved to cache key %r (ttl: %d)', cache_key, cache_ttl)
    return convert.Raw(result_json)


@app.route('/<version>/profile/<path:identifier>', methods=['GET'])
@flask_extras.json_service()
def get_profile_identifier(identifier):
    """
    Gets the user for the given identifier

    Request:
        GET /v2/profile/ricardovice

    Response:
        {
            "id": 12345678,
            "display_name": "Bob Brown",
            "aliases": ["bob", "bob@example.com"],
            "image_url": null
        }

    """
    identifier = urllib.unquote(identifier)
    account = accounts.get_handler(identifier).account
    if not account.can_make_requests:
        # Don't provide public profiles of disabled accounts.
        # XXX: It's very important that this error matches the other "not found" error.
        raise errors.ResourceNotFound('That account does not exist')
    session = auth.get_session()
    if session:
        if account.key in session.account.blocked_by:
            raise errors.ResourceNotFound('That account does not exist')
        g.public_options['view_account'] = session.account
    g.public_options['include_extras'] = True
    return account


@app.route('/<version>/profile/<identifier>/chat/joins', methods=['POST'])
@flask_extras.json_service()
@auth.authed_request()
def post_profile_identifier_chat_joins(session, identifier):
    text = flask_extras.get_parameter('text')
    if not text:
        raise errors.InvalidArgument('Missing text')
    owner = models.Account.resolve(identifier)
    if not owner:
        raise errors.InvalidArgument('Invalid identifier')
    if owner.key == session.account_key:
        # Don't notify creator if they're the one joining.
        return {'success': True}
    logging.debug('Notifying %d of join by %d', owner.key.id(), session.account_id)
    hub = notifs.Hub(owner)
    hub.emit(notifs.ON_CHAT_JOIN,
        channel_id=str(owner.key.id()),
        joiner=session.account,
        owner=owner,
        text=text)
    return {'success': True}


@app.route('/<version>/profile/<identifier>/chat/mentions', methods=['POST'])
@flask_extras.json_service()
@auth.authed_request()
def post_profile_identifier_chat_mentions(session, identifier):
    text = flask_extras.get_parameter('text')
    if not text:
        raise errors.InvalidArgument('Missing text')
    mentions = identifiers.find_mentions(text)
    plus_ids = set(flask_extras.get_parameter_list('identifier'))
    if not mentions and not plus_ids:
        return {'success': True}
    owner_future = models.Account.resolve_async(identifier)
    key_futures = [(m, models.Account.resolve_key_async(m)) for m in mentions | plus_ids]
    # Track both unique account keys and which identifiers were used to get them.
    mention_dict = collections.defaultdict(list)
    for m, f in key_futures:
        mention_key = f.get_result()
        if not mention_key or mention_key == session.account_key:
            continue
        mention_dict[mention_key].append(m)
    owner = owner_future.get_result()
    if not owner:
        raise errors.InvalidArgument('Invalid identifier')
    futures = []
    for mention_key in mention_dict.iterkeys():
        logging.debug('Notifying %d of mention in chat message by %d',
                      mention_key.id(), session.account_id)
        hub = notifs.Hub(mention_key)
        if any(m in mentions for m in mention_dict[mention_key]):
            event_name = notifs.ON_CHAT_MENTION
        else:
            # The account was not mentioned in the text.
            event_name = notifs.ON_CHAT_MESSAGE
        f = hub.emit_async(event_name,
            channel_id=str(owner.key.id()),
            owner=owner,
            sender=session.account,
            text=text)
        futures.append(f)
    _wait_all(futures)
    return {'success': True}


@app.route('/<version>/profile/<identifier>/chunks/<token>', methods=['GET'])
@flask_extras.json_service()
def get_profile_identifier_chunks_token(identifier, token):
    """
    Returns a single public chunk.
    """
    sender = accounts.get_handler(identifier)
    chunk = models.Chunk.get_by_token(sender.account, token)
    if not chunk:
        raise errors.ResourceNotFound('That chunk does not exist')
    result = {
        'stream_id': chunk.key.parent().id(),
        'chunk': chunk,
        'profile': sender.account,
        'receiver_identifier': None,
    }
    # If this chunk is in a 1:1 stream, expose the receiver's first phone number.
    stream_entity = chunk.key.parent().get()
    others = [p for p in stream_entity.participants if p.account != chunk.sender]
    if len(others) == 1:
        account = others[0].account.get()
        for identity_key in account.identifiers:
            identifier, identifier_type = identifiers.parse(identity_key.id())
            # TODO: Add e-mail once it's possible to use it for logging in.
            if identifier_type not in (identifiers.PHONE,):
                continue
            result['receiver_identifier'] = identifier
            break
    return result


@app.route('/<version>/profile/<identifier>/chunks/<token>', methods=['POST'])
@flask_extras.json_service()
def post_profile_identifier_chunks_token(identifier, token):
    """
    Updates a public chunk.
    """
    chunk = models.Chunk.get_by_token(identifier, token)
    if not chunk:
        raise errors.ResourceNotFound('That chunk does not exist')
    stream_entity = chunk.key.parent().get()
    others = filter(lambda p: p.account != chunk.sender, stream_entity.participants)
    if len(others) != 1:
        raise errors.ForbiddenAction('Cannot modify that chunk')
    stream = streams.MutableStream(others[0], stream_entity)
    # Update played state on behalf of the receiver.
    if flask_extras.get_flag('played'):
        was_unplayed = not stream.is_played
        stream.set_played_until(chunk.end, report=False)
        stream._report('played', duration=chunk.duration / 1000.0, unplayed=was_unplayed)
    return {'success': True}


@app.route('/<version>/profile/<identifier>/comments/', methods=['GET'])
@flask_extras.json_service()
def get_profile_identifier_comments(identifier):
    try:
        limit = int(flask_extras.get_parameter('limit') or 50)
        assert 0 < limit <= 500
    except:
        raise errors.InvalidArgument('Invalid limit')
    account_key = models.Account.resolve_key(identifier)
    if not account_key:
        raise errors.ResourceNotFound('That account does not exist')
    session = auth.get_session()
    if session and account_key in session.account.blocked_by:
        raise errors.ResourceNotFound('That account does not exist')
    q = models.ContentComment.query()
    q = q.filter(models.ContentComment.creator == account_key)
    q = q.order(-models.ContentComment.created)
    cursor_urlsafe = flask_extras.get_parameter('cursor')
    cursor = datastore_query.Cursor(urlsafe=cursor_urlsafe)
    comment_list, next_cursor, more = q.fetch_page(limit, start_cursor=cursor)
    # Look up the content that comments were for.
    content_keys = set(c.key.parent() for c in comment_list)
    lookup = {c.key: c for c in ndb.get_multi(content_keys) if c}
    # Build the result.
    data = []
    for comment in comment_list:
        content_key = comment.key.parent()
        content = lookup.get(content_key)
        if not content:
            logging.warning('Could not load content for comment %d.%s',
                            content_key.id(), comment.key.id())
            continue
        data.append({'comment': comment, 'content': content})
    return {'cursor': next_cursor.urlsafe() if more else None, 'data': data}


@app.route('/<version>/profile/<identifier>/content/<tag>/', methods=['GET'])
@flask_extras.json_service()
def get_profile_identifier_content_tag(identifier, tag):
    if models.Content.is_tag_unlisted(tag):
        raise errors.UnsupportedEndpoint()
    try:
        limit = int(flask_extras.get_parameter('limit') or 50)
        assert 0 < limit <= 500
    except:
        raise errors.InvalidArgument('Invalid limit')
    account_key = models.Account.resolve_key(identifier)
    if not account_key:
        raise errors.ResourceNotFound('That account does not exist')
    session = auth.get_session()
    if session and account_key in session.account.blocked_by:
        raise errors.ResourceNotFound('That account does not exist')
    q = models.Content.query()
    q = q.filter(models.Content.creator == account_key)
    q = q.filter(models.Content.tags == tag)
    q = q.order(-models.Content.created)
    cursor_urlsafe = flask_extras.get_parameter('cursor')
    cursor = datastore_query.Cursor(urlsafe=cursor_urlsafe)
    #content_list, next_cursor, more = q.fetch_page(limit, start_cursor=cursor)
    content_list, next_cursor, more = [], None, False
    # Look up extra data for the content list.
    session_key = session.account_key if session else None
    lookup, votes = models.Content.decorate(content_list,
                                            include_creator=True,
                                            include_related=True,
                                            for_account_key=session_key)
    # Build the result.
    data = []
    for i, content in enumerate(content_list):
        result = {
            'content': content,
            'creator': lookup[content.creator],
            'related_to': lookup.get(content.related_to),
            'voted': votes[i] is not None,
        }
        data.append(result)
    return {'cursor': next_cursor.urlsafe() if more else None, 'data': data}


@app.route('/<version>/profile/<identifier>/followers/', methods=['GET'])
@flask_extras.json_service()
def get_profile_identifier_followers(identifier):
    account_key = models.Account.resolve_key(identifier)
    if not account_key:
        raise errors.ResourceNotFound('That account does not exist')
    session = auth.get_session()
    if session:
        g.public_options['view_account'] = session.account
        if account_key in session.account.blocked_by:
            raise errors.ResourceNotFound('That account does not exist')
    start_cursor = datastore_query.Cursor(urlsafe=flask_extras.get_parameter('cursor'))
    results, next_cursor, more = models.AccountFollow.fetch_followers_page(
        account_key, 50, start_cursor=start_cursor)
    g.public_options['include_extras'] = True
    return {'cursor': next_cursor.urlsafe() if more else None, 'data': results}


@app.route('/<version>/profile/<identifier>/following/', methods=['GET'])
@flask_extras.json_service()
def get_profile_identifier_following(identifier):
    account_key = models.Account.resolve_key(identifier)
    if not account_key:
        raise errors.ResourceNotFound('That account does not exist')
    session = auth.get_session()
    if session:
        g.public_options['view_account'] = session.account
        if account_key in session.account.blocked_by:
            raise errors.ResourceNotFound('That account does not exist')
    start_cursor = datastore_query.Cursor(urlsafe=flask_extras.get_parameter('cursor'))
    results, next_cursor, more = models.AccountFollow.fetch_following_page(
        account_key, 50, start_cursor=start_cursor)
    g.public_options['include_extras'] = True
    return {'cursor': next_cursor.urlsafe() if more else None, 'data': results}


@app.route('/<version>/profile/<identifier>/original/', methods=['GET'])
@flask_extras.json_service()
def get_profile_identifier_original(identifier):
    account_key = models.Account.resolve_key(identifier)
    if not account_key:
        raise errors.ResourceNotFound('That account does not exist')
    sort = flask_extras.get_parameter('sort') or 'recent'
    try:
        limit = int(flask_extras.get_parameter('limit') or 20)
        assert 0 < limit <= 500
    except:
        raise errors.InvalidArgument('Invalid limit')
    cursor_urlsafe = flask_extras.get_parameter('cursor') or None
    # Attempt to get the data from cache.
    cache_key = 'user_original_%s_%d_%s_%d_%s' % (g.api_version, account_key.id(), sort, limit, cursor_urlsafe)
    cache_json = memcache.get(cache_key)
    if cache_json:
        logging.debug('Loaded cache key %r', cache_key)
        return convert.Raw(cache_json)
    # Query the data from the datastore.
    q = models.Content.query()
    q = q.filter(models.Content.creator == account_key)
    q = q.filter(models.Content.tags == 'original')
    # TODO: Support more sort orders?
    if sort == 'recent':
        q = q.order(-models.Content.created)
        cache_ttl = 3600
    else:
        raise errors.InvalidArgument('Invalid sort value')
    cursor = datastore_query.Cursor(urlsafe=cursor_urlsafe)
    content_list, next_cursor, more = q.fetch_page(limit, start_cursor=cursor)
    # Look up extra data for the content list.
    poster_futures = []
    related_futures = []
    for content in content_list:
        poster_key = content.first_related_creator or ndb.Key('Account', config.REACTION_CAM_ID)
        poster_futures.append(poster_key.get_async())
        q = models.Content.query()
        q = q.filter(models.Content.related_to == content.key)
        q = q.filter(models.Content.tags == 'reaction')
        q = q.order(-models.Content.sort_bonus)
        related_futures.append(q.fetch_async(10))
    # Build the result and a payload to cache.
    result = {'cursor': next_cursor.urlsafe() if more else None, 'data': []}
    for i, content in enumerate(content_list):
        if poster_futures[i]:
            poster = poster_futures[i].get_result()
        else:
            poster = None
        related_source = related_futures[i].get_result()
        related = []
        for r in related_source:
            if 'repost' in r.tags:
                continue
            if any(r.creator == rr.creator for rr in related):
                continue
            related.append(r)
            if len(related) >= 5:
                break
        result['data'].append({
            'content': content,
            'poster': poster,
            'related': related,
        })
    cache_json = convert.to_json(result, **g.public_options)
    memcache.set(cache_key, cache_json, time=cache_ttl)
    logging.debug('Saved to cache key %r (ttl: %d)', cache_key, cache_ttl)
    return result


@app.route('/<version>/profile/<identifier>/pay/', methods=['POST'])
@flask_extras.json_service()
@auth.authed_request()
def post_profile_identifier_pay(session, identifier):
    # TODO: Require client to provide last known transaction to avoid double spending.
    try:
        amount = int(flask_extras.get_parameter('amount'))
        assert amount > 0
    except:
        raise errors.InvalidArgument('Invalid amount')
    account_key = models.Account.resolve_key(identifier)
    if not account_key:
        raise errors.ResourceNotFound('That account does not exist')
    if account_key == session.account_key:
        raise errors.InvalidArgument('Cannot pay self')
    if account_key in session.account.blocked_by:
        raise errors.ResourceNotFound('That account does not exist')
    if not session.account.wallet:
        raise errors.InvalidArgument('Insufficient currency')
    account = account_key.get()
    if account.wallet:
        wallet_key = account.wallet
    else:
        logging.debug('Creating wallet for %d', account_key.id())
        _, wallet = models.Wallet.create_async(account_key).get_result()
        wallet_key = wallet.key
    comment = flask_extras.get_parameter('comment')
    if comment is not None:
        comment = comment.strip()[:50].strip() or None
    future = models.Wallet.create_tx_async(
        session.account_key, session.account.wallet,
        wallet_key, amount, u'Payment (%s)' % (comment,))
    tx = future.get_result()
    w1, _, w2, _ = tx().get_result()
    # Track payments.
    event = events.WalletPaymentV1(
        account_key=session.account_key,
        receiver_id=account_key.id(),
        amount=amount,
        account_new_balance=w1.balance,
        receiver_new_balance=w2.balance)
    futures = [event.report_async()]
    # Notify receiver.
    text = ('Sent %d Coins' % (amount,)) if amount != 1 else 'Sent 1 Coin'
    try:
        h = threads.Handler(session.account_key)
        f = h.message_identifiers_async([account], 'currency', text,
                                        {'amount': amount, 'comment': comment},
                                        allow_nonuser_type=True)
        futures.append(f)
    except:
        logging.exception('Failed to send payment message from %d to %d',
            session.account_id, account.key.id())
    _wait_all(futures)
    is_bot = 'ReactionCam/1337' in request.headers.get('User-Agent', '')
    if not is_bot:
        text = u'%s just gave %s to %s! (%s)' % (
            slack_api.admin(session.account),
            ('%d Coins' % (amount,)) if amount != 1 else '1 Coin',
            slack_api.admin(account),
            comment)
        slack_api.message(channel='#review', text=text, hook_id='reactioncam')
    return {
        'wallet': w1,
    }


@app.route('/<version>/profile/<identifier>/releases/', methods=['GET'])
@flask_extras.json_service()
def get_profile_identifier_releases(identifier):
    account = models.Account.resolve(identifier)
    if not account:
        raise errors.ResourceNotFound('That account does not exist')
    # Attempt to get the data from cache.
    cache_key = 'user_releases_%s_%d' % (g.api_version, account.key.id())
    cache_json = memcache.get(cache_key)
    if cache_json:
        logging.debug('Loaded cache key %r', cache_key)
        return convert.Raw(cache_json)
    content_ids = account.properties.get('releases')
    if not isinstance(content_ids, list) or not all(isinstance(cid, (int, long)) for cid in content_ids):
        logging.warning('Account %d had unexpected value for "releases": %r', account.key.id(), content_ids)
        content_ids = []
    # Query the data from the datastore.
    content_list = ndb.get_multi(ndb.Key('Content', cid) for cid in content_ids)
    # Build the result and a payload to cache.
    result = {'cursor': None, 'data': []}
    for i, content in enumerate(content_list):
        if not content:
            logging.warning('Content did not exist: %d', content_ids[i])
            continue
        if content.creator != account.key:
            logging.warning('Content creator mismatch: %d != %d', content.creator.id(), account.key.id())
            continue
        result['data'].append(content)
    cache_json = convert.to_json(result, **g.public_options)
    cache_ttl = 120
    memcache.set(cache_key, cache_json, time=cache_ttl)
    logging.debug('Saved to cache key %r (ttl: %d)', cache_key, cache_ttl)
    return result


@app.route('/<version>/profile/<identifier>/requests/', methods=['POST'])
@flask_extras.json_service()
@auth.authed_request()
def post_profile_identifier_requests(session, identifier):
    # Note: This endpoint sends content requests as private messages.
    account_key = models.Account.resolve_key(identifier)
    if not account_key:
        raise errors.ResourceNotFound('That account does not exist')
    if account_key in session.account.blocked_by:
        raise errors.ResourceNotFound('That account does not exist')
    content_key, content = utils.get_or_create_content_from_request()
    if not content_key:
        raise errors.ResourceNotFound('That content does not exist')
    if not content:
        account, content = ndb.get_multi([account_key, content_key])
    else:
        account = account_key.get()
    if not account:
        raise errors.ResourceNotFound('That account does not exist')
    if not content:
        raise errors.ResourceNotFound('That content does not exist')
    # TODO: Prevent request spamming.
    futures = []
    content_request = models.ContentRequest(
        content=content_key,
        requested_by=session.account_key,
        parent=account_key)
    f = content_request.put_async()
    futures.append(f)
    # Send the request as a message.
    if content.title:
        text = u'Reaction request: %s' % (content.title,)
    else:
        text = u'Reaction request'
    data = {
        'id': content_key.id(),
        'thumb_url': content.thumb_url,
        'title': content.title,
        'url': content.original_url or content.video_url,
    }
    thread = threads.Handler(session.account_key)
    f = thread.message_identifiers_async([account], 'request', text, data,
                                         allow_nonuser_type=True)
    futures.append(f)
    _wait_all(futures)
    return {'success': True}


@app.route('/<version>/profile/<identifier>/pay/top', methods=['GET'])
@flask_extras.json_service()
def get_profile_identifier_pay_top(identifier):
    account_key = models.Account.resolve_key(identifier)
    if not account_key:
        raise errors.ResourceNotFound('That account does not exist')
    cache_key = 'pay_top_%s_%d' % (g.api_version, account_key.id())
    cache_json = memcache.get(cache_key)
    if cache_json:
        logging.debug('Loaded cache key %r', cache_key)
        return convert.Raw(cache_json)
    query = QUERY_HIGHEST_PAYING_ACCOUNTS % (account_key.id(), 10)
    logging.debug('Running query:%s', query)
    rows = list(bigquery_client.query(query).rows())
    account_keys = [ndb.Key('Account', int(r.account_id)) for r in rows
                    if int(r.account_id) != config.REACTION_CAM_ID]
    lookup = {a.key.id(): a for a in ndb.get_multi(account_keys) if a}
    top_list = []
    for r in rows:
        account = lookup.get(int(r.account_id))
        if not account:
            logging.debug('Could not include account %r', r.account_id)
            continue
        top_list.append({
            'account': account,
            'total_amount': int(r.total_amount),
        })
    result = {'data': top_list}
    cache_json = convert.to_json(result, **g.public_options)
    cache_ttl = 28800
    memcache.set(cache_key, cache_json, time=cache_ttl)
    logging.debug('Saved to cache key %r (ttl: %d)', cache_key, cache_ttl)
    return result


@app.route('/<version>/publicstreams/<invite_token>', methods=['GET'])
@flask_extras.json_service()
def get_publicstreams_invite_token(invite_token):
    return streams.get_by_invite_token(invite_token)


@app.route('/<version>/purchase/', methods=['POST'])
@flask_extras.json_service()
@auth.authed_request()
def post_purchase(session):
    transaction_ids = set(flask_extras.get_parameter_list('purchase_id'))
    receipt_data = flask_extras.get_parameter('receipt')
    try:
        base64.b64decode(receipt_data)
    except:
        raise errors.InvalidArgument('Invalid receipt data provided')
    # Exchange opaque receipt data for readable JSON with Apple's payment server.
    try:
        data = apple.itunes(receipt_data)
    except:
        logging.exception('Could not connect to iTunes')
        raise errors.ExternalError('Could not connect to iTunes')
    environment = data['environment']
    # Ensure that the account has a wallet to transfer to.
    if session.account.wallet:
        wallet_key = session.account.wallet
    else:
        logging.debug('Creating wallet for %d', session.account_key.id())
        _, wallet = models.Wallet.create_async(session.account_key).get_result()
        wallet_key = wallet.key
    # Handle all transactions in the response.
    completed_transaction_ids = set()
    updated_wallet = None
    purchased_items = []
    for purchase in data['receipt']['in_app']:
        try:
            t_id = purchase['transaction_id']
            if t_id not in transaction_ids:
                logging.debug('Ignoring transaction id %r', t_id)
                continue
            product_id = purchase['product_id']
            product = config.ITUNES_PRODUCTS.get(product_id)
            if not product:
                logging.error('Unknown product %r', product_id)
                continue
            count = int(purchase['quantity'])
            if count < 1:
                logging.error('Bad quantity %r', count)
                continue
            if product['type'] == 'currency':
                # Create wallet for amount, then transfer it to the account wallet.
                total_amount = product['amount'] * count
                comment = u'Apple (%s) %s x%d (%s) at %s' % (
                    environment, product_id, count, t_id,
                    purchase['purchase_date_pst'])
                future = models.Wallet.create_and_transfer_async(
                    session.account_key, wallet_key,
                    'itunes_%s' % (purchase['original_transaction_id'],),
                    total_amount, comment)
                updated_wallet = future.get_result()
            else:
                logging.error('Unknown product type %r', product['type'])
                continue
            purchased_items.append(u'%d× %s' % (count, product_id))
            completed_transaction_ids.add(t_id)
        except:
            logging.exception('Failed to complete purchase %r', purchase)
    # Log if we didn't complete all transactions.
    missing_transaction_ids = transaction_ids - completed_transaction_ids
    if missing_transaction_ids:
        logging.warning('Did not handle all transactions: %r', missing_transaction_ids)
    # TODO: Remove this.
    logging.debug('Data from Apple: %r', data)
    if purchased_items:
        text = u'%s just bought %s!' % (
            slack_api.admin(session.account),
            ', '.join(purchased_items))
        slack_api.message(channel='#general', text=text, hook_id='reactioncam')
    # Let the client know the full list of purchases that were completed in this call.
    return {
        'completed_purchase_ids': list(completed_transaction_ids),
        'wallet': updated_wallet,
    }


@app.route('/<version>/register', methods=['POST'])
@flask_extras.json_service()
def post_register():
    """
    Registers a new user account.

    Request:
        POST /v1/register?username=bob&password=top_secret

    Response:
        {
            "access_token": "RLDvsbckw7tJJCiCPzU9bF",
            "refresh_token": "pArhTbEs8ex1f79vAqxR2",
            "token_type": "bearer",
            "expires_in": 3600,
            "status": "active",
            "account": {
                "id": 12345678,
                "display_name": "Bob Brown",
                "aliases": ["bob"]
            }
        }

    """
    client_id, _ = auth.get_client_details()
    username = flask_extras.get_parameter('username') or None
    if username:
        username, identifier_type = identifiers.parse(username)
        if identifier_type != identifiers.USERNAME:
            # A user may not use this endpoint to add a phone number/e-mail.
            raise errors.InvalidArgument('A valid username must be provided')
    birthday = flask_extras.get_parameter('birthday') or None
    if birthday:
        birthday = models.Account.parse_birthday(birthday)
    display_name = flask_extras.get_parameter('display_name') or None
    gender = flask_extras.get_parameter('gender') or None
    image = request.files.get('image') or None
    # Create the account with provided parameters.
    user = accounts.create(username,
        birthday=birthday,
        display_name=display_name,
        gender=gender,
        image=image,
        last_active_client=request.headers.get('User-Agent'))
    # Add password to the account if one was provided.
    challenge = 'none'
    password = flask_extras.get_parameter('password')
    if password:
        challenge = 'password'
        user.set_password(password)
    report.user_registered(user.account_id, auth_identifier=None, challenge=challenge,
                           status=user.account.status)
    # Track logins.
    taskqueue.add(url='/_ah/jobs/track_login',
                  countdown=20,
                  params={'account_id': user.account_id,
                          'client_id': client_id},
                  queue_name=config.INTERNAL_QUEUE)
    # Support adding participants in conjunction with registering.
    g.public_options['include_extras'] = True
    return utils.set_up_session(user, participants=flask_extras.get_parameter_list('stream_participant'))


@app.route('/<version>/remote', methods=['POST'])
@flask_extras.json_service()
def post_remote():
    session = auth.get_session()
    if not session:
        raise errors.InvalidAccessToken()
    token = flask_extras.get_parameter('token')
    if not token:
        raise errors.InvalidArgument('Missing token')
    data = flask_extras.get_parameter('data')
    try:
        json.loads(data)
    except:
        raise errors.InvalidArgument('Invalid data')
    template = '{"account_id": %d, "device_token": %s, "notification": null, "data": %s}'
    json_string = template % (session.account_id, json.dumps(token), data)
    push_service.post_async(json_string).get_result()
    return {'success': True}


@app.route('/<version>/report/<event_name>', methods=['POST'])
@flask_extras.json_service()
def post_report(event_name):
    if not re.match(r'^([A-Z][a-z]+)+V\d+$', event_name):
        # Ensure that the event name follows naming convention to avoid potential abuse.
        raise errors.InvalidArgument('Invalid event type')
    event_type = getattr(events, event_name, None)
    if not event_type or not issubclass(event_type, events.RogerEventV1):
        raise errors.InvalidArgument('Invalid event type')
    session = auth.get_session()
    event = event_type(session.account_id if session else None)
    for field in event_type._fields:
        # TODO: Better type support.
        value = flask_extras.get_parameter(field.name)
        if value is not None:
            setattr(event, field.name, value)
    event.report()
    return {'success': True}


@app.route('/<version>/requests/', methods=['POST'])
@flask_extras.json_service()
@auth.authed_request()
def post_requests(session):
    # Note: This endpoint sends content requests as notifications.
    identifier_list = flask_extras.get_parameter_list('identifier')
    account_keys = models.Account.resolve_keys(identifier_list)
    if not all(account_keys):
        raise errors.InvalidArgument('One or more "identifier" arguments were invalid')
    account_keys = filter(lambda k: k not in session.account.blocked_by, account_keys)
    if not account_keys:
        return {'success': True}
    content_key, content = utils.get_or_create_content_from_request()
    if not content_key:
        raise errors.ResourceNotFound('That content does not exist')
    taskqueue.add(url='/_ah/jobs/content_request_send',
                  params={'requester_id': session.account_id,
                          'account_id': [k.id() for k in account_keys],
                          'comment': flask_extras.get_parameter('comment') or '',
                          'content_id': content_key.id()},
                  queue_name=config.INTERNAL_QUEUE)
    return {'success': True}


@app.route('/<version>/requests/public/', methods=['POST', 'PUT'])
@flask_extras.json_service()
@auth.authed_request()
def post_requests_public(session):
    content_key, _ = utils.get_or_create_content_from_request()
    if not content_key:
        raise errors.ResourceNotFound('That content does not exist')
    tags = models.Content.parse_tags(flask_extras.get_parameter('tags') or '')
    tags.add('pending')
    if tags == {'pending'}:
        raise errors.InvalidArgument('Specify at least one tag')
    if tags != {'default', 'pending'}:
        raise errors.NotSupported('Can only create requests with tag "default" for now')
    # TODO: Dedupe/rate limit.
    request = models.ContentRequestPublic(
        content=content_key,
        requested_by=session.account_key,
        sort_index=models.Content.get_sort_index(),
        tags=tags)
    request.put()
    return request


@app.route('/<version>/requests/public/<tags>/', methods=['GET'])
@flask_extras.json_service()
def get_requests_public_tags(tags):
    # Parse and validate list of user specified tags separated by "+".
    tags = models.Content.parse_tags(tags, allow_restricted=True, separator='+')
    # TODO: Validate individual tags.
    if not tags:
        raise errors.UnsupportedEndpoint()
    current_user_only = flask_extras.get_flag('mine')
    if not current_user_only:
        tags.add('approved')
    # Sort.
    sort = flask_extras.get_parameter('sort') or 'hot'
    # Limit.
    try:
        limit = int(flask_extras.get_parameter('limit') or 50)
        assert 0 < limit <= 500
    except:
        raise errors.InvalidArgument('Invalid limit')
    # Cursor.
    cursor_urlsafe = flask_extras.get_parameter('cursor')
    # Load from cache.
    if current_user_only:
        cache_key = None
        cache_json = None
    else:
        tags_string = '+'.join(sorted(tags))
        cache_key = 'public_requests_%s_%s_%s_%d_%s' % (g.api_version, tags_string, sort, limit, cursor_urlsafe)
        cache_json = memcache.get(cache_key)
    if cache_json:
        logging.debug('Loaded cache key %r', cache_key)
        return convert.Raw(cache_json)
    # No cache available.
    q = models.ContentRequestPublic.query()
    if current_user_only:
        session = auth.get_session()
        if not session:
            raise errors.ForbiddenAction('Must be authenticated to request own public requests')
        q = q.filter(models.ContentRequestPublic.requested_by == session.account_key)
    for tag in tags:
        q = q.filter(models.ContentRequestPublic.tags == tag)
    # TODO: Respect sort parameter.
    q = q.order(-models.ContentRequestPublic.sort_index)
    cursor = datastore_query.Cursor(urlsafe=cursor_urlsafe)
    request_list, next_cursor, more = q.fetch_page(limit, start_cursor=cursor)
    lookup_keys = set()
    lookup_keys.update(r.content for r in request_list)
    lookup_keys.update(r.wallet for r in request_list if r.wallet)
    lookup = {e.key: e for e in ndb.get_multi(lookup_keys)}
    data = []
    for request in request_list:
        reward = lookup[request.wallet].balance if request.wallet else None
        data.append({
            'content': lookup[request.content],
            'request': request.public(reward=reward, version=g.api_version),
        })
    result = {
        'cursor': next_cursor.urlsafe() if more else None,
        'data': data,
    }
    if cache_key:
        cache_json = convert.to_json(result, **g.public_options)
        cache_ttl = 300
        memcache.set(cache_key, cache_json, time=cache_ttl)
        logging.debug('Saved to cache key %r (ttl: %d)', cache_key, cache_ttl)
    return result


@app.route('/<version>/requests/public/<int:request_id>', methods=['GET'])
@flask_extras.json_service()
@auth.authed_request()
def get_requests_public_request_id(session, request_id):
    request_key = ndb.Key('ContentRequestPublic', request_id)
    entry_key = models.ContentRequestPublicEntry.resolve_key(request_key, session.account_key)
    request, entry = ndb.get_multi([request_key, entry_key])
    if not request:
        raise errors.ResourceNotFound('That request does not exist')
    if entry and entry.content:
        if request.wallet:
            content, wallet, reaction = ndb.get_multi([request.content, request.wallet, entry.content])
        else:
            content, reaction = ndb.get_multi([request.content, entry.content])
            wallet = None
    elif request.wallet:
        content, wallet = ndb.get_multi([request.content, request.wallet])
        reaction = None
    else:
        content = request.content.get()
        wallet = None
        reaction = None
    reward = wallet.balance if wallet else None
    if entry:
        if entry.status == 'open' and request.closed:
            status = 'closed'
        else:
            status = entry.status
    else:
        status = 'closed' if request.closed else 'open'
    return {
        'content': content,
        'entry': entry,
        'reaction': reaction,
        'request': request.public(reward=reward, version=g.api_version),
        'status': status,
        'status_reason': entry.status_reason if entry else None,
    }


@app.route('/<version>/requests/public/<int:request_id>', methods=['POST'])
@flask_extras.json_service()
@auth.authed_request()
def post_requests_public_request_id(session, request_id):
    request = models.ContentRequestPublic.get_by_id(request_id)
    if not request:
        raise errors.ResourceNotFound('That request does not exist')
    if session.account_key != request.requested_by:
        raise errors.ForbiddenAction('Must be creator of request')
    if not request.properties:
        request.properties = {}
    properties = flask_extras.get_json_properties(
        'properties',
        apply_to_dict=request.properties)
    if properties:
        request.put()
    return {'success': True}


@app.route('/<version>/requests/public/<int:request_id>/details', methods=['GET'])
@flask_extras.json_service()
@auth.authed_request()
def get_requests_public_request_id_details(session, request_id):
    request_key = ndb.Key('ContentRequestPublic', request_id)
    request_future, active_future, inactive_future = (
        _get_request_and_wallet_async(request_key),
        _get_request_entries_with_content_async(request_key, 'active'),
        _get_request_entries_with_content_async(request_key, 'inactive'))
    request, wallet, content = request_future.get_result()
    if not request:
        raise errors.ResourceNotFound('That request does not exist')
    if request.requested_by != session.account_key:
        raise errors.ForbiddenAction('Can only look at own request details')
    active, inactive = active_future.get_result(), inactive_future.get_result()
    # Sort active and inactive separately by YouTube views descending.
    key_fn = lambda (e, c): c.youtube_views
    active.sort(key=key_fn, reverse=True)
    inactive.sort(key=key_fn, reverse=True)
    # Create an API data structure for the sorted entries, with inactive ones last.
    entries = [{'entry': e, 'reaction': c, 'status': e.status} for e, c in active + inactive]
    reward = wallet.balance if wallet else None
    return {
        'content': content,
        'entries': entries,
        'request': request.public(reward=reward, version=g.api_version),
        'total_active_entries': len(active),
        'total_coins_added': wallet.total_received if wallet else 0,
        'total_coins_spent': wallet.total_sent if wallet else 0,
        'total_inactive_entries': len(inactive),
        'total_sponsored_views': sum((e['reaction'].youtube_views or 0) for e in entries),
        'total_youtube_views': content.youtube_reaction_views or 0,
    }


@app.route('/<version>/requests/public/<int:request_id>/entry', methods=['POST', 'PUT'])
@flask_extras.json_service()
@auth.authed_request()
def post_requests_public_request_id_entry(session, request_id):
    req = ndb.Key('ContentRequestPublic', request_id).get()
    if not req:
        raise errors.ResourceNotFound('That request does not exist')
    content_id = flask_extras.get_parameter('content_id')
    if content_id is not None:
        try:
            content_id = int(content_id)
        except:
            raise errors.InvalidArgument('Invalid content_id value')
    youtube_id = flask_extras.get_parameter('youtube_id')
    if content_id and youtube_id:
        raise errors.InvalidArgument('Cannot specify both content_id and youtube_id')
    if content_id:
        content = models.Content.get_by_id(content_id)
        if not content:
            raise errors.ResourceNotFound('That content does not exist')
        if content.creator != session.account_key:
            raise errors.ForbiddenAction('That content was created by another account')
        if content.related_to != req.content:
            raise errors.InvalidArgument('That content is not a reaction to the correct video')
        if content.request and content.request != req.key:
            raise errors.InvalidArgument('That content is an entry to another request')
        elif not content.request:
            content.request = req.key
            content.put()
    elif youtube_id:
        if not session.account.has_service('youtube'):
            raise errors.ForbiddenAction('Account is not connected to YouTube')
        if not session.account.youtube_channel_id:
            raise errors.ServerError('Account does not have a YouTube channel id')
        # Try to find existing Content and also get the YouTube video metadata.
        content_future, video_future = (
            models.Content.get_by_youtube_id_async(youtube_id),
            youtube.get_video_async(youtube_id, statistics=True))
        content, video = content_future.get_result(), video_future.get_result()
        if not video:
            raise errors.ResourceNotFound('That YouTube video does not exist')
        # Verify that the YouTube video is owned by this user.
        if video['snippet']['channelId'] != session.account.youtube_channel_id:
            raise errors.ForbiddenAction('That YouTube video is not on your channel')
        # TODO: Check the description of the video for the required link.
        if content:
            # TODO: Ways to deal with this corner case?
            if content.creator != session.account_key:
                raise errors.ForbiddenAction('That YouTube video is under another account')
            if content.related_to != req.content:
                raise errors.InvalidArgument('That YouTube video is not a reaction to the correct video')
            if content.request and content.request != req.key:
                raise errors.InvalidArgument('That YouTube video is an entry to another request')
            needs_put = False
            if not content.request:
                content.request = req.key
                needs_put = True
            youtube_views = int(video['statistics']['viewCount'])
            if content.youtube_views != youtube_views:
                content.youtube_views = youtube_views
                content.youtube_views_updated = datetime.utcnow()
                needs_put = True
            if needs_put:
                content.put()
        else:
            # Create a hidden Content object representing the video.
            # TODO: Consider more ways to match YT video/Content before creating new.
            thumb_url = None
            if 'medium' in video['snippet']['thumbnails']:
                thumb_url = video['snippet']['thumbnails']['medium']['url']
            content = models.Content.new(allow_restricted_tags=True,
                creator=session.account_key,
                related_to=req.content,
                request=req.key,
                tags=['is hidden'],
                thumb_url=thumb_url,
                title=video['snippet']['title'],
                youtube_id_history=[video['id']],
                youtube_views=int(video['statistics']['viewCount']),
                youtube_views_updated=datetime.utcnow())
            content.put()
    else:
        content = None
    entry, did_change = models.ContentRequestPublicEntry.update(
        (req.key, session.account_key), content,
        reset=flask_extras.get_flag('reset'))
    if did_change:
        hub = notifs.Hub(session.account)
        hub.emit(notifs.ON_PUBLIC_REQUEST_UPDATE, request_id=req.key.id())
    return {'success': True}


@app.route('/<version>/services', methods=['GET'])
@flask_extras.json_service()
@auth.authed_request()
def get_services(session):
    category = 'service' if g.api_version >= 21 else None
    # Add refresh token forwards to every connect URL.
    # TODO: Add refresh token client side.
    s = services.get_connected_and_featured(session.account, category=category)
    token = session.create_refresh_token()
    def inject_refresh_token(service):
        if not service.connect_url:
            return service
        data = service.public(version=g.api_version)
        # XXX: Hack to support Android and iOS at the same time...
        client, _ = flask_extras.parse_user_agent()
        if client == 'ios' and service.key.id() == 'ifttt_exit_area':
            data['connect_url'] = 'https://ifttt.com/applets/96WTTjaL/embed?redirect_uri=rogerbot%3A//ifttt'
        params = urllib.urlencode({'refresh_token': token, 'to': data['connect_url']})
        data['connect_url'] = 'https://rogertalk.com/forward?' + params
        return data
    return {
        'data': map(inject_refresh_token, s),
    }


@app.route('/<version>/services/<service_id>/groups', methods=['GET'])
@flask_extras.json_service()
@auth.authed_request()
def get_services_service_id_groups(session, service_id):
    team_id = flask_extras.get_parameter('team_id')
    auth_key = models.ServiceAuth.resolve_key((session.account, service_id, team_id))
    auth = auth_key.get()
    if not auth:
        raise errors.ResourceNotFound('No connection with the specified service found')
    if service_id != 'slack':
        raise errors.NotSupported('Only Slack is supported for now')
    pending_1 = slack_api.get_channel_list(auth.access_token, _async=True)
    pending_2 = slack_api.get_group_list(auth.access_token, _async=True)
    slack_channels = pending_1.get_result()
    slack_groups = pending_2.get_result()
    if not slack_channels or not slack_groups:
        raise errors.ExternalError('Failed to get channel/group list')
    # TODO: Sort?
    groups = []
    for item in slack_channels['channels']:
        if not item['is_member']:
            continue
        groups.append({
            'identifier': auth.build_identifier(item['id']),
            'title': '#%s' % (item['name'],),
        })
    for item in slack_groups['groups']:
        if item['is_mpim']:
            continue
        groups.append({
            'identifier': auth.build_identifier(item['id']),
            'title': '#%s' % (item['name'],),
        })
    return {'data': groups}


@app.route('/<version>/services/<service_id>/invite', methods=['POST'])
@flask_extras.json_service()
@auth.authed_request()
def post_services_service_id_invite(session, service_id):
    if service_id not in ('email', 'fika'):
        raise errors.NotSupported('Invites for that service are not supported')
    team_id = flask_extras.get_parameter('team_id')
    auth_key = models.ServiceAuth.resolve_key((session.account, service_id, team_id))
    team_key = models.ServiceTeam.resolve_key((service_id, team_id))
    if not team_key:
        raise errors.NotSupported('This functionality requires a team')
    if service_id == 'email' and team_key.id() in config.PUBLIC_EMAIL_DOMAINS:
        raise errors.ForbiddenAction('This functionality is not available to your team')
    auth_info, team = ndb.get_multi([auth_key, team_key])
    if not auth_info:
        raise errors.ResourceNotFound('No connection with the specified service found')
    client_id, _ = auth.get_client_details()
    # TODO: If this list grows, we need to create jobs for batches of invites.
    user_list = flask_extras.get_parameter_list('identifier')
    for user in user_list:
        if service_id == 'fika':
            # fika.io invites specifically are not restricted by service team.
            user_id = user
            # TODO: Support SMS invites?
            _, identifier_type = identifiers.parse(user_id)
            if identifier_type != identifiers.EMAIL:
                raise errors.NotSupported('Can only create invites for email')
        else:
            user_id = auth_info.build_identifier(user)
        invited = accounts.get_or_create(user_id, status='invited')
        params = dict(
            invited_name=invited.display_name,
            inviter_name=session.account.display_name,
            team_name=team.name,
            team_slug=team.slug_with_fallback)
        if service_id == 'email':
            params.update(sender=identifiers.email(auth_info.identifier))
        elif service_id == 'fika':
            # This account should be added to the team it's been invited to.
            invited.add_identifier(auth_info.build_identifier(str(invited.key.id())))
        localize.send_email(client_id, 'invite',
            to=identifiers.email(user_id),
            to_name=invited.display_name,
            sender_name=session.account.display_name,
            **params)
        streams.get_or_create(session.account, [invited.account])
    return {'success': True}


@app.route('/<version>/services/<service_id>/users', methods=['GET'])
@flask_extras.json_service()
@auth.authed_request()
def get_services_service_id_users(session, service_id):
    team_id = flask_extras.get_parameter('team_id')
    auth_key = models.ServiceAuth.resolve_key((session.account, service_id, team_id))
    auth = auth_key.get()
    if not auth:
        raise errors.ResourceNotFound('No connection with the specified service found')
    if not auth.service_team:
        raise errors.NotSupported('Only team connections are supported')
    users = []
    if service_id in ('email', 'fika'):
        q = models.ServiceAuth.query(models.ServiceAuth.service_team == auth.service_team)
        for user_auth, user_account in q.map(_get_auth_and_account):
            if user_account.location_info and user_account.share_location:
                timezone = user_account.location_info.timezone
            else:
                timezone = None
            users.append({
                'identifier': user_auth.identifier,
                'display_name': user_account.display_name,
                'timezone': timezone,
            })
    elif service_id == 'slack':
        result = slack_api.get_users(auth.access_token)
        if not result:
            raise errors.ExternalError('Failed to get user list')
        for member in result['members']:
            if member['deleted'] or member['is_bot'] or member['id'] == 'USLACKBOT':
                continue
            users.append({
                'identifier': auth.build_identifier(member['id']),
                'display_name': member['profile']['real_name'] or member['name'],
                'timezone': member['tz'],
            })
    else:
        raise errors.NotSupported('Getting users for that service is not supported')
    users.sort(key=lambda u: u['display_name'].upper())
    return {'data': users}


@app.route('/<version>/streams', methods=['GET'])
@flask_extras.json_service()
@auth.authed_request(allow_nonactive=True, set_view_account=True)
def get_streams(session):
    """
    Gets all streams for the authenticated account.

    Request:
        GET /v1/streams
        Authorization: Bearer 5ZVhC41gMAqjpno3ELKc5pLtcAbQAPO1bWJf6hCMFW8T20s

    Response:
        {
            "data": [
                {
                    ...
                }
            ]
        }

    """
    cursor = flask_extras.get_parameter('cursor')
    if g.api_version >= 32:
        max_results = 50
    else:
        max_results = 10
    s, c = streams.get_recent(session.account, max_results=max_results, cursor=cursor)
    result = {'data': s}
    if g.api_version >= 7:
        result['cursor'] = c
    return result


@app.route('/<version>/streams', methods=['POST'])
@flask_extras.json_service()
@auth.authed_request(set_view_account=True)
def post_streams(session):
    """
    Create or get a stream for the provided participant(s). If a stream exists for the
    provided set of participants (including the current user) and title, that one will be
    returned instead of creating another one.

    Request:
        POST /v29/streams?duration=1234&participant=1234567890&participant=%2B16461234321
        Authorization: Bearer 5ZVhC41gMAqjpno3ELKc5pLtcAbQAPO1bWJf6hCMFW8T20s
        payload=<Binary data>

    Response:
        {...}

    """
    image_file = request.files.get('image')
    show_in_recents = flask_extras.get_flag('show_in_recents')
    flags = flask_extras.get_flag_dict('shareable', 'solo')
    reason = flask_extras.get_parameter('reason') or 'streams'
    title = flask_extras.get_parameter('title')

    invite_token = flask_extras.get_parameter('invite_token')
    service_content_id = flask_extras.get_parameter('service_content_id')
    if invite_token or service_content_id:
        # Sanity check the form parameters.
        if invite_token and service_content_id:
            raise errors.InvalidArgument('Cannot specify both invite_token and service_content_id')
        if flask_extras.get_flag('export') is not None:
            raise errors.InvalidArgument('Cannot specify export when joining')
        if flask_extras.get_parameter('participant') is not None:
            raise errors.InvalidArgument('Cannot specify participants when joining')
        if 'shareable' in flags:
            raise errors.InvalidArgument('Cannot specify shareable when joining')
        if 'solo' in flags:
            raise errors.InvalidArgument('Cannot specify solo when joining')
        if title is not None:
            raise errors.InvalidArgument('Cannot set title when joining')
        if image_file:
            raise errors.InvalidArgument('Cannot set image when joining')

    if invite_token:
        stream = streams.get_by_invite_token(invite_token)
        if not stream:
            raise errors.ResourceNotFound('Invalid invite token')
        stream = stream.join(session.account, reason=reason)
    elif service_content_id:
        # Look up the third-party group.
        service_key, team_key, resource = models.Service.parse_identifier(service_content_id)
        if not session.account.is_on_team(service_key, team_key):
            raise errors.ForbiddenAction('Must be authenticated to that service/team')
        # TODO: Support multiple types of services dynamically.
        if service_key.id() == 'slack':
            auth = session.account.get_auth_key(service_key, team_key).get()
            # TODO: Put this API call elsewhere!
            if resource.startswith('C'):
                info = slack_api.get_channel_info(resource, auth.access_token)
                channel = info.get('channel')
            elif resource.startswith('G'):
                info = slack_api.get_group_info(resource, auth.access_token)
                channel = info.get('group')
            else:
                raise errors.InvalidArgument('Unrecognized resource')
            if not info['ok']:
                raise errors.ExternalError('Something went wrong')
            join_params = dict(
                service_members=channel['members'],
                title='#%s' % (channel['name'],))
        elif service_key.id() in ('email', 'fika') and resource == '*':
            raise errors.ForbiddenAction('This functionality is not available to your team')
        else:
            raise errors.NotSupported('Unsupported service_content_id')
        autocreate = flask_extras.get_flag('autocreate')
        if autocreate is not None:
            join_params['autocreate'] = autocreate
        stream = streams.join_service_content(session.account, service_content_id, **join_params)
        if not stream:
            raise errors.ResourceNotFound('Invalid resource')
    else:
        # Extract one or more participants from the request parameters.
        participants = map(accounts.Resolver.parse, flask_extras.get_parameter_list('participant'))
        # Convert participant information to account keys.
        params = {'origin_account': session.account}
        if reason == 'voicemail':
            params['create_status'] = 'voicemail'
        account_keys = [p.get_or_create_account_key(**params) for p in participants]
        if image_file:
            image = files.upload(image_file.filename, image_file.stream, persist=True)
        else:
            image = None
        if g.api_version >= 16:
            # As of version 16 the streams are created visible by default.
            create_hidden = show_in_recents == False
        else:
            create_hidden = True
        if title:
            title = title.strip()
        stream = streams.get_or_create(session.account, account_keys,
                                       create_hidden=create_hidden, image=image,
                                       reason=reason, title=title, **flags)
    sent_payload = utils.upload_and_send(stream)
    if not sent_payload and show_in_recents:
        stream.show()
    return stream


@app.route('/<version>/streams/<stream_id>', methods=['DELETE'])
@flask_extras.json_service()
@auth.authed_request()
def delete_streams_stream_id(session, stream_id):
    """
    Removes the current user from a stream. This
    will not affect the stream for other users.
    """
    stream = streams.get_by_id(session.account, stream_id, disable_autojoin=True)
    stream.leave()
    return {'success': True}


@app.route('/<version>/streams/<stream_id>', methods=['GET'])
@flask_extras.json_service()
@auth.authed_request(allow_nonactive=True, set_view_account=True)
def get_streams_stream_id(session, stream_id):
    """
    Gets a single stream for the authenticated account.

    Request:
        GET /v1/streams/12345678
        Authorization: Bearer 5ZVhC41gMAqjpno3ELKc5pLtcAbQAPO1bWJf6hCMFW8T20s

    Response:
        {...}

    """
    return streams.get_by_id(session.account, stream_id)


@app.route('/<version>/streams/<stream_id>', methods=['POST'])
@flask_extras.json_service()
@auth.authed_request(set_view_account=True)
def post_streams_stream_id(session, stream_id):
    """
    Updates the properties of a stream.

    Request:
        POST /v1/streams/12345678?played_until=123456789000
        Authorization: Bearer 5ZVhC41gMAqjpno3ELKc5pLtcAbQAPO1bWJf6hCMFW8T20s

    Response:
        {...}

    """
    stream = streams.get_by_id(session.account, stream_id)
    played_until = flask_extras.get_parameter('played_until')
    if played_until:
        stream.set_played_until(played_until)
    image = request.files.get('image')
    if image and not stream.featured:
        # TODO: Right now a featured stream silently ignores this request.
        path = files.upload(image.filename, image.stream, persist=True)
        stream.set_image(path)
    shareable = flask_extras.get_flag('shareable')
    if shareable is not None:
        stream.set_shareable(shareable)
    status = flask_extras.get_parameter('status')
    if status:
        estimated_duration = flask_extras.get_parameter('status_estimated_duration')
        if estimated_duration:
            try:
                estimated_duration = int(estimated_duration)
            except ValueError:
                raise errors.InvalidArgument('Estimated duration must be an integer (ms)')
        stream.announce_status(status, estimated_duration)
    title = flask_extras.get_parameter('title')
    if title is not None and not stream.featured:
        # TODO: Right now a featured stream silently ignores this request.
        stream.set_title(title.strip())
    visible = flask_extras.get_flag('visible')
    if visible is not None:
        if visible:
            stream.show()
        else:
            stream.hide()
    return stream


@app.route('/<version>/streams/<stream_id>/attachments/<attachment_id>', methods=['DELETE'])
@flask_extras.json_service()
@auth.authed_request(set_view_account=True)
def delete_streams_stream_id_attachments_attachment_id(session, stream_id, attachment_id):
    """
    Removes an attachment from the specified stream.
    """
    stream = streams.get_by_id(session.account, stream_id)
    stream.remove_attachment(attachment_id)
    return stream


@app.route('/<version>/streams/<stream_id>/attachments/<attachment_id>', methods=['POST'])
@flask_extras.json_service()
@auth.authed_request(set_view_account=True)
def post_streams_stream_id_attachments_attachment_id(session, stream_id, attachment_id):
    """
    Sets an attachment on the specified stream.
    """
    try:
        data = json.loads(flask_extras.get_parameter('data'))
    except:
        raise errors.InvalidArgument('The data specified is not valid JSON')
    if not isinstance(data, dict):
        raise errors.InvalidArgument('Attachment data must be an object')
    # Also allow file uploads.
    # TODO: Add some kind of restriction to this.
    # TODO: Decide if uploads should actually be persisted.
    for key, field in request.files.iteritems(multi=True):
        if key in data:
            raise errors.AmbiguousArgument('"%s" was specified more than once' % (key,))
        path = files.upload(field.filename, field.stream, persist=True)
        data[key] = files.storage_url(path)
    # Store the attachment object on the stream.
    stream = streams.get_by_id(session.account, stream_id)
    stream.set_attachment(attachment_id, do_not_bump=g.api_version >= 28, **data)
    return stream


@app.route('/<version>/streams/<stream_id>/buzz', methods=['POST'])
@flask_extras.json_service()
@auth.authed_request(set_view_account=True)
def post_streams_stream_id_buzz(session, stream_id):
    """
    Sends an buzz notification to the specified stream.

    Request:
        POST /v1/streams/12345678/
        Authorization: Bearer 5ZVhC41gMAqjpno3ELKc5pLtcAbQAPO1bWJf6hCMFW8T20s

    Response:
        {...}

    """
    stream = streams.get_by_id(session.account, stream_id)
    stream.buzz()
    return stream


@app.route('/<version>/streams/<stream_id>/chunks', methods=['GET'])
@flask_extras.json_service()
@auth.authed_request(allow_nonactive=True, set_view_account=True)
def get_streams_stream_id_chunks(session, stream_id):
    """
    Gets the chunks in the specified stream.

    Request:
        GET /v1/streams/12345678/chunks
        Authorization: Bearer 5ZVhC41gMAqjpno3ELKc5pLtcAbQAPO1bWJf6hCMFW8T20s

    Response:
        {"data": ...}

    """
    chunk_filter = flask_extras.get_parameter('filter')
    if chunk_filter == 'reacted':
        # TODO: Confirm account is a participant in the stream.
        stream_key = ndb.Key('Stream', int(stream_id))
        threshold = datetime.utcnow() - config.CHUNK_MAX_AGE
        q = models.Chunk.query(models.Chunk.start >= threshold, ancestor=stream_key)
        q = q.filter(models.Chunk.reaction_keys == session.account_key)
        q = q.order(models.Chunk.start)
        return dict(data=q.fetch())
    stream = streams.get_by_id(session.account, stream_id, all_chunks=True)
    return dict(data=stream.chunks)


@app.route('/<version>/streams/<stream_id>/chunks', methods=['POST'])
@flask_extras.json_service()
@auth.authed_request(set_view_account=True)
def post_streams_stream_id_chunks(session, stream_id):
    """
    Sends a chunk to the specified stream.

    Request:
        POST /v29/streams/12345678/chunks?duration=1234
        Authorization: Bearer 5ZVhC41gMAqjpno3ELKc5pLtcAbQAPO1bWJf6hCMFW8T20s
        payload=<Binary data>

    Response:
        {...}

    """
    stream = streams.get_by_id(session.account, stream_id)
    if not utils.upload_and_send(stream):
        raise errors.InvalidArgument('Chunk data missing from request')
    return stream


@app.route('/<version>/streams/<stream_id>/chunks/<chunk_id>', methods=['GET'])
@flask_extras.json_service()
@auth.authed_request(allow_nonactive=True, set_view_account=True)
def get_streams_stream_id_chunks_chunk_id(session, stream_id, chunk_id):
    # TODO: Confirm account is a participant in the stream.
    stream_key = ndb.Key('Stream', int(stream_id))
    chunk = models.Chunk.get_by_id(int(chunk_id), parent=stream_key)
    if not chunk:
        raise errors.ResourceNotFound('That chunk does not exist')
    return chunk


@app.route('/<version>/streams/<stream_id>/chunks/<chunk_id>', methods=['POST'])
@flask_extras.json_service()
@auth.authed_request(allow_nonactive=True, set_view_account=True)
def post_streams_stream_id_chunks_chunk_id(session, stream_id, chunk_id):
    """Update metadata for an individual chunk."""
    stream = streams.get_by_id(session.account, stream_id)
    chunk = None
    if g.api_version >= 36:
        reaction_type = flask_extras.get_parameter('reaction')
    else:
        reaction_flag = flask_extras.get_flag('reaction')
        if reaction_flag is None:
            reaction_type = None
        else:
            reaction_type = u'👍' if reaction_flag else ''
    if reaction_type is not None:
        if not reaction_type:
            # Empty string means unset the reaction.
            reaction_type = None
        chunk = stream.react_to_chunk(chunk_id, reaction_type)
    if not chunk:
        raise errors.NotSupported('Only reaction is supported')
    return chunk


@app.route('/<version>/streams/<stream_id>/chunks/<chunk_id>', methods=['DELETE'])
@flask_extras.json_service()
@auth.authed_request()
def delete_streams_stream_id_chunks_chunk_id(session, stream_id, chunk_id):
    """
    Delete an individual chunk.
    """
    stream = streams.get_by_id(session.account, stream_id)
    stream.remove_chunk(chunk_id)
    return {'success': True}


@app.route('/<version>/streams/<stream_id>/image', methods=['DELETE'])
@flask_extras.json_service()
@auth.authed_request(set_view_account=True)
def delete_streams_stream_id_image(session, stream_id):
    """
    Removes the custom image from the stream.

    Request:
        DELETE /v7/streams/12345678/image
        Authorization: Bearer 5ZVhC41gMAqjpno3ELKc5pLtcAbQAPO1bWJf6hCMFW8T20s

    Response:
        {...}

    """
    stream = streams.get_by_id(session.account, stream_id)
    if stream.featured:
        # TODO: Right now a featured stream silently ignores this request.
        return stream
    stream.set_image(None)
    return stream


@app.route('/<version>/streams/<stream_id>/participants', methods=['DELETE'])
@flask_extras.json_service()
@auth.authed_request(set_view_account=True)
def delete_streams_stream_id_participants(session, stream_id):
    """
    Removes one or more participants from the stream.

    Request:
        DELETE /v1/streams/12345678/participants?participant=129381&participant=124982
        Authorization: Bearer 5ZVhC41gMAqjpno3ELKc5pLtcAbQAPO1bWJf6hCMFW8T20s

    Response:
        {...}

    """
    stream = streams.get_by_id(session.account, stream_id, disable_autojoin=True)
    if stream.featured:
        # TODO: Right now a featured stream silently ignores this request.
        return stream
    stream.eject(flask_extras.get_parameter_list('participant'))
    return stream


@app.route('/<version>/streams/<stream_id>/participants', methods=['POST'])
@flask_extras.json_service()
@auth.authed_request(set_view_account=True)
def post_streams_stream_id_participants(session, stream_id):
    """
    Adds one or more participants to the specified stream.

    Request:
        POST /v1/streams/12345678/participants?participant=87654321
        Authorization: Bearer 5ZVhC41gMAqjpno3ELKc5pLtcAbQAPO1bWJf6hCMFW8T20s

    Response:
        {...}

    """
    stream = streams.get_by_id(session.account, stream_id)
    participants = map(accounts.Resolver.parse, flask_extras.get_parameter_list('participant'))
    account_keys = set()
    for participant in participants:
        account_key = participant.get_or_create_account_key()
        if not account_key:
            # TODO: Create temporary accounts for participants without accounts.
            raise errors.NotSupported('Cannot add external users to a stream (yet)')
        account_keys.add(account_key)
    stream.invite(account_keys)
    return stream


@app.route('/<version>/streams/<stream_id>/title', methods=['DELETE'])
@flask_extras.json_service()
@auth.authed_request(set_view_account=True)
def delete_streams_stream_id_title(session, stream_id):
    """
    Clears the custom title for the stream.

    Request:
        DELETE /v7/streams/12345678/title
        Authorization: Bearer 5ZVhC41gMAqjpno3ELKc5pLtcAbQAPO1bWJf6hCMFW8T20s

    Response:
        {...}

    """
    stream = streams.get_by_id(session.account, stream_id)
    if stream.featured:
        # TODO: Right now a featured stream silently ignores this request.
        return stream
    stream.set_title(None)
    return stream


@app.route('/<version>/suggested', methods=['GET'])
@flask_extras.json_service()
def get_suggested():
    try:
        limit = int(flask_extras.get_parameter('limit') or 10)
        assert 1 <= limit <= 100
    except:
        raise errors.InvalidArgument('Invalid limit parameter')
    cache_key = 'suggested_%s_%d' % (g.api_version, limit)
    cache_json = memcache.get(cache_key)
    if cache_json:
        logging.debug('Loaded cache key %r', cache_key)
        return convert.Raw(cache_json)
    # Seed suggested users from featured feed.
    q = models.Content.query()
    q = q.filter(models.Content.tags == 'featured')
    q = q.order(-models.Content.sort_index)
    content_list = q.fetch(limit * 5)
    if len(content_list) > limit:
        content_list = random.sample(content_list, limit)
    account_keys = set(c.creator for c in content_list)
    result = {'data': models.Account.resolve_list(account_keys)}
    cache_json = convert.to_json(result, **g.public_options)
    # TODO: Consider raising this to 1 hour.
    cache_ttl = 600
    memcache.set(cache_key, cache_json, time=cache_ttl)
    logging.debug('Saved to cache key %r (ttl: %d)', cache_key, cache_ttl)
    return result


@app.route('/<version>/tags', methods=['GET'])
@flask_extras.json_service()
def get_tags():
    return {'data': [
        {'label': '#trending', 'tag': 'trending'},
        {'label': '#reacttothis', 'tag': 'reacttothis'},
        {'label': '#gottalent', 'tag': 'trending agt'},
        {'label': '#music', 'tag': 'trending music'},
        {'label': '#challenge', 'tag': 'trending challenge'},
        {'label': '#scary', 'tag': 'trending scary'},
    ]}


@app.route('/<version>/threads/', methods=['GET'])
@flask_extras.json_service()
@auth.authed_request(update_last_active=False)
def get_threads(session):
    cursor_urlsafe = flask_extras.get_parameter('cursor')
    cursor = datastore_query.Cursor(urlsafe=cursor_urlsafe)
    handler = threads.Handler(session.account_key)
    thread_list, next_cursor = handler.get_recent_threads(cursor=cursor)
    bb = session.account.blocked_by
    return {
        'cursor': next_cursor.urlsafe() if next_cursor else None,
        'data': [t for t in thread_list if not any(a.account in bb for a in t.accounts)],
    }


@app.route('/<version>/threads/', methods=['PUT'])
@flask_extras.json_service()
@auth.authed_request()
def put_threads(session):
    # TODO: Block support.
    f = memcache._CLIENT.incr_async('thread_created_%d' % (session.account_id,), initial_value=0)
    identifier_list = flask_extras.get_parameter_list('identifier')
    handler = threads.Handler(session.account_key)
    thread = handler.get_or_create(identifier_list)
    try:
        threads_created = f.get_result()
        if threads_created in (8, 10, 15, 20, 30, 40, 50, 60, 70, 80, 90, 100, 125, 150, 200, 300, 500, 750, 1000):
            slack_api.message(channel='#abuse', hook_id='reactioncam', text='{} created {} threads recently'.format(
                slack_api.admin(session.account),
                threads_created))
    except:
        logging.exception('Failed to count threads created.')
    return thread


@app.route('/<version>/threads/<thread_id>/', methods=['GET'])
@flask_extras.json_service()
@auth.authed_request()
def get_threads_thread_id(session, thread_id):
    handler = threads.Handler(session.account_key)
    thread = handler.get_by_id(thread_id)
    if any(a.account in session.account.blocked_by for a in thread.accounts):
        raise errors.ResourceNotFound('Thread not found')
    return thread


@app.route('/<version>/threads/<thread_id>/', methods=['POST'])
@flask_extras.json_service()
@auth.authed_request()
def post_threads_thread_id(session, thread_id):
    handler = threads.Handler(session.account_key)
    thread = handler.get_by_id(thread_id)
    if any(a.account in session.account.blocked_by for a in thread.accounts):
        raise errors.ResourceNotFound('Thread not found')
    new_seen_until = flask_extras.get_parameter('seen_until')
    if new_seen_until:
        thread.set_seen_until(new_seen_until)
    visible = flask_extras.get_flag('visible')
    if visible is not None:
        if visible:
            thread.show()
        else:
            thread.hide()
    return thread


@app.route('/<version>/threads/<thread_id>/messages/', methods=['GET'])
@flask_extras.json_service()
@auth.authed_request()
def get_threads_thread_id_messages(session, thread_id):
    cursor_urlsafe = flask_extras.get_parameter('cursor')
    cursor = datastore_query.Cursor(urlsafe=cursor_urlsafe)
    handler = threads.Handler(session.account_key)
    thread, messages, next_cursor = handler.get_recent_messages(thread_id, cursor=cursor)
    if any(a.account in session.account.blocked_by for a in thread.accounts):
        raise errors.ResourceNotFound('Thread not found')
    return {
        'cursor': next_cursor.urlsafe() if next_cursor else None,
        'data': messages,
    }


@app.route('/<version>/threads/<thread_id>/messages/', methods=['POST'])
@flask_extras.json_service()
@auth.authed_request()
def post_threads_thread_id_messages(session, thread_id):
    try:
        data = flask_extras.get_parameter('data')
        if data:
            data = json.loads(data)
        else:
            data = {}
    except:
        raise errors.InvalidArgument('Invalid message data')
    message_type = flask_extras.get_parameter('type')
    text = flask_extras.get_parameter('text')
    handler = threads.Handler(session.account_key)
    flags = flask_extras.get_flag_dict('keep_hidden_from_sender')
    thread = handler.message(thread_id, message_type, text, data, account=session.account, **flags)
    return thread


@app.route('/<version>/top/accounts', methods=['GET'])
@app.route('/<version>/top/accounts/<category>', methods=['GET'])
@flask_extras.json_service()
def get_top_accounts_category(category='votes'):
    if category not in ('creators', 'first', 'payments', 'votes'):
        raise errors.ResourceNotFound()
    tag = flask_extras.get_parameter('tag')
    if tag:
        if category in ('creators', 'payments'):
            raise errors.InvalidArgument('Cannot specify tag for %r' % (category,))
        if ' ' in tag or len(tag) > 20:
            raise errors.InvalidArgument('Invalid tag')
        tag = tag.lower()
    # Calculate time range to get ranks for.
    tz = pytz.timezone('America/New_York')
    midnight = pytz.utc.localize(datetime.utcnow()).astimezone(tz)
    midnight = midnight.replace(hour=0, minute=0, second=0, microsecond=0)
    midnight = midnight.astimezone(pytz.utc)
    ts_max = midnight.strftime('%Y-%m-%d %H:%M')
    midnight += timedelta(days=-7)
    ts_min = midnight.strftime('%Y-%m-%d %H:%M')
    # Attempt to load from cache.
    if tag:
        cache_key = 'top_accounts_%s_%s_%s_%s_%s' % (g.api_version, category, tag, ts_min, ts_max)
    else:
        cache_key = 'top_accounts_%s_%s_%s_%s' % (g.api_version, category, ts_min, ts_max)
    result_json = memcache.get(cache_key)
    if result_json:
        logging.debug('Loaded cache key %r', cache_key)
        return convert.Raw(result_json)
    top_list = []
    if category == 'creators':
        account_keys = [ndb.Key('Account', aid) for aid in config.TOP_CREATOR_IDS]
        accounts = ndb.get_multi(account_keys[:30])
        for account in accounts:
            top_list.append({
                'account': account,
                'score': account.content_reaction_count,
            })
    else:
        # Get the data from BigQuery.
        if tag:
            if category == 'first':
                query = QUERY_TOP_ACCOUNTS_FIRST_TAG
            elif category == 'votes':
                query = QUERY_TOP_ACCOUNTS_VOTES_TAG
            query = query % (tag.replace('"', '\\"'), ts_min, ts_max, 50)
        else:
            if category == 'first':
                query = QUERY_TOP_ACCOUNTS_FIRST
            elif category == 'payments':
                query = QUERY_TOP_ACCOUNTS_PAYMENTS
            elif category == 'votes':
                query = QUERY_TOP_ACCOUNTS_VOTES
            query = query % (ts_min, ts_max, 50)
        logging.debug('Running query:%s', query)
        rows = list(bigquery_client.query(query).rows())
        account_keys = [ndb.Key('Account', int(r.account_id)) for r in rows]
        lookup = {a.key.id(): a for a in ndb.get_multi(account_keys) if a}
        for r in rows:
            account = lookup.get(int(r.account_id))
            if not account:
                logging.debug('Could not include account %r', r.account_id)
                continue
            top_list.append({
                'account': account,
                'score': int(r.score),
            })
    result = {'data': top_list}
    # Save data in cache before returning it.
    result_json = convert.to_json(result, **g.public_options)
    cache_ttl = 86400
    memcache.set(cache_key, result_json, time=cache_ttl)
    logging.debug('Saved to cache key %r (ttl: %d)', cache_key, cache_ttl)
    return convert.Raw(result_json)


@app.route('/<version>/youtube/auth', methods=['POST'])
@flask_extras.json_service()
@auth.authed_request(set_view_account=True)
def post_youtube_auth(session):
    # TODO: Support logging in with this endpoint?
    code = flask_extras.get_parameter('code')
    if not code:
        raise errors.MissingArgument('Missing code parameter')
    data, profile = youtube.auth_async(code).get_result()
    user_id = profile['sub']
    handler = accounts.get_handler(session.account)
    email = profile.get('email')
    if email and profile['email_verified'] and not handler.has_identifier(email):
        try:
            handler.add_identifier(email, notify_change=False, notify_connect=False)
        except errors.AlreadyExists:
            logging.warning('Email %s already associated with another account', email)
        except:
            logging.exception('Failed to add email %s to account %d',
                email, session.account_key.id())
    identifier = models.Service.build_identifier('youtube', None, user_id)
    if not handler.has_identifier(identifier):
        handler.add_identifier(identifier, reclaim=True)
    refresh_token = data.get('refresh_token')
    handler.connect_service('youtube', None, user_id, notify=False,
        access_token=data.get('access_token'),
        expires_in=data.get('expires_in'),
        refresh_token=refresh_token,
        token_type=data.get('token_type'))
    try:
        channels_future = youtube.get_channels_async(refresh_token)
        channels = channels_future.get_result()
        logging.debug('Channel(s): %r', channels)
        channel = channels[0]
        needs_update = (channel['id'] != handler.youtube_channel_id or
                        channel['thumb_url'] != handler.youtube_channel_thumb_url or
                        channel['title'] != handler.youtube_channel_title or
                        channel['views'] != handler.youtube_channel_views or
                        channel['subs'] != handler.youtube_subs)
        if needs_update:
            handler.account.youtube_channel_id = channel['id']
            handler.account.youtube_channel_thumb_url = channel['thumb_url']
            handler.account.youtube_channel_title = channel['title']
            handler.account.youtube_channel_views = channel['views']
            handler.account.youtube_subs = channel['subs']
            handler.account.youtube_subs_updated = datetime.utcnow()
            handler.account.put()
    except:
        logging.exception('Failed to get YouTube channel.')
    g.public_options['include_extras'] = True
    return session


@app.route('/<version>/youtube/videos/', methods=['GET'])
@flask_extras.json_service()
@auth.authed_request()
def get_youtube_videos(session):
    if not session.account.has_service('youtube'):
        raise errors.InvalidCredentials('Not connected to YouTube')
    auth = session.account.get_auth_key('youtube').get()
    if not auth or not auth.refresh_token:
        raise errors.ServerError('Could not load credentials')
    limit = flask_extras.get_parameter('limit')
    if limit:
        try:
            limit = int(limit)
            assert 1 <= limit <= 100
        except:
            raise errors.InvalidArgument('Invalid limit')
    else:
        limit = 10
    videos = youtube.get_videos_async(auth.refresh_token, limit=limit).get_result()
    return {'data': videos}


@app.route('/<version>/youtube/upload', methods=['POST'])
@flask_extras.json_service()
@auth.authed_request()
def post_youtube_upload(session):
    try:
        content_id = int(flask_extras.get_parameter('content_id'))
    except:
        raise errors.InvalidArgument('Invalid content_id value')
    if not session.account.has_service('youtube'):
        raise errors.InvalidCredentials('Not connected to YouTube')
    auth_future = session.account.get_auth_key('youtube').get_async()
    content = models.Content.get_by_id(content_id)
    if not content or content.creator != session.account_key:
        raise errors.ResourceNotFound('Content could not be found')
    auth = auth_future.get_result()
    if not auth or not auth.refresh_token:
        raise errors.ServerError('Could not load credentials')
    youtube.upload_async(session.account, content, token=auth.refresh_token).get_result()
    return {'success': True}


@ndb.tasklet
def _add_task_async(task, **kwargs):
    yield task.add_async(**kwargs)


def _b64hash(value):
    if isinstance(value, unicode):
        value = value.encode('utf-8')
    return hashlib.md5(value).digest().encode('base64').strip('\n=')


@ndb.transactional_tasklet
def _change_content_comment_count_async(creator, comment, count):
    content_key = comment.key.parent()
    content = yield content_key.get_async()
    content.add_comment_count(creator, count=count)
    old_tags = set(content.tags)
    content.add_tags(re.findall('#(\\w+)', comment.text))
    new_tags = set(content.tags) - old_tags
    if new_tags:
        logging.debug('Added tags from comment: %s', ', '.join(new_tags))
    logging.debug('Updated content %d comment count to %d',
                  content_key.id(), content.comment_count)
    yield content.put_async()
    raise ndb.Return(content)


def _content_cache_load(cache_key, session_key=None):
    cache_json = memcache.get(cache_key)
    if not cache_json:
        return None
    logging.debug('Loaded cache key %r', cache_key)
    return _load_and_inject_votes(cache_json, session_key)


def _content_cache_save(cache_key, result_dict, cache_ttl=3600):
    cache_marker = config.CONTENT_CACHE_MARKER + str(result_dict['content'].key.id())
    cache_dict = dict(result_dict, voted=cache_marker)
    cache_json = convert.to_json(cache_dict, **g.public_options)
    memcache.set(cache_key, cache_json, time=cache_ttl)
    logging.debug('Saved to cache key %r (ttl: %d)', cache_key, cache_ttl)


def _filter_content(content_list, hide_flagged=False):
    if hide_flagged:
        content_list = [c for c in content_list if 'flagged' not in c.tags]
    return content_list


@ndb.tasklet
def _get_auth_and_account(auth):
    account = yield auth.key.parent().get_async()
    raise ndb.Return((auth, account))


@ndb.tasklet
def _get_request_and_wallet_async(request_key):
    request = yield request_key.get_async()
    if not request:
        raise ndb.Return((None, None, None))
    if request.wallet:
        wallet, content = yield ndb.get_multi_async([request.wallet, request.content])
    else:
        wallet = None
        content = yield request.content.get_async()
    raise ndb.Return((request, wallet, content))


@ndb.tasklet
def _get_request_entries_with_content_async(request_key, status):
    CRPE = models.ContentRequestPublicEntry
    q = CRPE.query()
    q = q.filter(CRPE.request == request_key)
    q = q.order(-CRPE.created)
    q = q.filter(CRPE.status == status)
    entries = yield q.fetch_async()
    if not entries:
        raise ndb.Return([])
    contents = yield ndb.get_multi_async([e.content for e in entries])
    raise ndb.Return(list(zip(entries, contents)))


@ndb.tasklet
def _get_single_feed_async(account_key, tag):
    account_future = account_key.get_async()
    q = models.Content.query()
    q = q.filter(models.Content.creator == account_key)
    q = q.filter(models.Content.tags == tag)
    q = q.order(-models.Content.created)
    content_list = yield q.fetch_async(limit=10)
    raise ndb.Return((account_future, content_list))


_TOP_ACCOUNTS_INDEX = None


@ndb.tasklet
def _get_top_accounts_index_async():
    global _TOP_ACCOUNTS_INDEX
    if _TOP_ACCOUNTS_INDEX:
        raise ndb.Return(_TOP_ACCOUNTS_INDEX)
    cache_key = 'profile_search_top_accounts'
    context = ndb.get_context()
    index_json = yield context.memcache_get(cache_key)
    if index_json:
        index = json.loads(index_json)
        _TOP_ACCOUNTS_INDEX = index
        logging.debug('Restored top accounts index from memcache')
        raise ndb.Return(index)
    q = models.Account.query()
    q = q.order(-models.Account.follower_count)
    accounts = yield q.fetch_async(500)
    index = []
    for account in accounts:
        username = account.username
        if not username:
            continue
        index.append([username, account.key.id()])
    _TOP_ACCOUNTS_INDEX = index
    index_json = json.dumps(index)
    yield context.memcache_set(cache_key, index_json, time=86400)
    logging.debug('Restored top accounts index from scratch (and set memcache)')
    raise ndb.Return(index)


def _handle_content_became_public(creator, content, related_to):
    event_v1 = events.ContentV1(
        account_key=creator.key,
        content_id=content.key.id(),
        related_id=content.related_to.id() if content.related_to else None,
        duration=content.duration / 1000,
        tags=content.tags)
    event_v2 = events.ContentV2(
        account=creator,
        content_id=content.key.id(),
        related_id=content.related_to.id() if content.related_to else None,
        duration=content.duration / 1000,
        tags=content.tags)
    futures = [event_v1.report_async(), event_v2.report_async()]
    # Notify original creator that their video was referenced.
    if related_to and 'reaction' in related_to.tags:
        # TODO: Consider if we should send this for non-reaction videos.
        handler = accounts.get_handler(related_to.creator)
        logging.debug('Notifying %s that their content was referenced by %s',
            handler.username, creator.username)
        future = handler.notifs.emit_async(
            notifs.ON_CONTENT_REFERENCED,
            content=content, creator=creator)
        futures.append(future)
    # Kick off a job that will notify any users that want to know about this reaction.
    task = taskqueue.Task(
        url='/_ah/jobs/content_became_public',
        params={'content_id': content.key.id(),
                'creator_id': creator.key.id(),
                'original_id': related_to.key.id() if related_to else ''})
    futures.append(_add_task_async(task, queue_name=config.INTERNAL_QUEUE))
    _wait_all(futures)


def _handle_content_was_deleted(creator, content, related_to):
    task = taskqueue.Task(
        countdown=5,
        url='/_ah/jobs/content_was_deleted',
        params={'content_id': content.key.id(),
                'creator_id': creator.key.id(),
                'original_id': related_to.key.id() if related_to else ''})
    task.add(queue_name=config.INTERNAL_QUEUE)


def _load_and_inject_votes(cache_json, session_key):
    vote_keys = []
    # Split the string so that the left part ends after '"voted":' and the right part
    # contains the content id to get the vote for (followed by a citation mark).
    pieces = cache_json.split('"' + config.CONTENT_CACHE_MARKER)
    for i, piece in enumerate(pieces):
        if i == 0:
            # Don't do anything to beginning of data.
            continue
        # The first citation mark will be the end of the content id.
        index = piece.index('"')
        # Remove the content id and citation mark.
        pieces[i] = piece[index + 1:]
        if session_key:
            vote_key = ndb.Key('ContentVote', int(piece[:index]), parent=session_key)
            vote_keys.append(vote_key)
    if vote_keys:
        votes = ['false' if v is None else 'true' for v in ndb.get_multi(vote_keys)]
    else:
        votes = ['false'] * (len(pieces) - 1)
    # Put the JSON back together with votes and return as-is, skipping conversion.
    chain = itertools.chain.from_iterable(itertools.izip(pieces, votes + ['']))
    return ''.join(chain)


@ndb.tasklet
def _upload_to_youtube_async(creator, content):
    auth = yield creator.get_auth_key('youtube').get_async()
    if not auth or not auth.refresh_token:
        logging.error('Could not load user YouTube credentials')
        raise ndb.Return(False)
    yield youtube.upload_async(creator, content, token=auth.refresh_token)
    raise ndb.Return(True)


@ndb.synctasklet
def _wait_all(futures):
    errors = 0
    for f in futures:
        try:
            yield f
        except Exception:
            logging.exception('Error in Future')
            errors += 1
    if errors:
        raise Exception('%d error(s) occurred' % (errors,))
