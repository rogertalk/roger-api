# -*- coding: utf-8 -*-

import json
import logging
import re
import urllib
import urlparse

from google.appengine.api import taskqueue, urlfetch
from google.appengine.ext import ndb

from flask import g, request

from roger import accounts, auth, bots, config, files, models, streams
from roger_common import errors, flask_extras, random


def get_or_create_content(*args, **kwargs):
    return get_or_create_content_async(*args, **kwargs).get_result()


@ndb.tasklet
def get_or_create_content_async(content_id=None, creator=None, url=None, duration=0,
                                thumb_url=None, title=None, video_url=None,
                                tags=['original'], **kwargs):
    if content_id:
        raise ndb.Return((ndb.Key('Content', content_id), None))
    key, content = None, None
    if url:
        url, thumb_url = normalize_content_urls(url, thumb_url)
        # Attempt to look up original content by its URL.
        content = yield models.Content.query(models.Content.original_url == url).get_async()
        if content:
            logging.debug('Found existing content %d for URL %r', content.key.id(), url)
            key = content.key
            # Workarounds for a lot of content being created with "YouTube" title etc.
            needs_put = False
            if duration and not content.duration:
                try:
                    content.duration = max(int(duration), 0)
                    if content.duration > 0:
                        logging.debug('Autofixing duration to %d', content.duration)
                        needs_put = True
                except:
                    logging.warning('Failed to parse duration from %r', duration)
            if thumb_url and not content.thumb_url:
                content.thumb_url = yield highres_thumb_url(thumb_url)
                logging.debug('Autofixing thumbnail to %r', content.thumb_url)
                needs_put = True
            if title and content.title == 'YouTube':
                title = models.Content.clean_title(title)
                if title != content.title:
                    slug = models.Content.make_slug('%s %s' % (title, random.base62(10)))
                    logging.debug('Autofixing content %d title (%r => %r) and slug (%r => %r)',
                                  key.id(), content.title, title, content.slug, slug)
                    content.slug = slug
                    content.title = title
                    needs_put = True
            if needs_put:
                yield content.put_async()
    # Create content if sufficient data is available.
    if not content and all([title, url]):
        title = models.Content.clean_title(title)
        logging.debug('TODO: Kick off job to import creator %r', creator)
        if thumb_url:
            thumb_url = yield highres_thumb_url(thumb_url)
        content = models.Content.new(
            creator=ndb.Key('Account', config.ANONYMOUS_ID),
            original_url=url,
            duration=duration,
            tags=tags,
            thumb_url=thumb_url or None,
            title=title,
            video_url=video_url or None,
            useragent=request.headers.get('User-Agent', 'unknown'),
            **kwargs)
        if not content.slug:
            # Autogenerate a slug that contains the content title.
            value = '%s %s' % (title, random.base62(10))
            content.slug = models.Content.make_slug(value)
        if not content.thumb_url:
            content.thumb_url = content.thumb_url_from_video_url()
        key = yield content.put_async()
        logging.debug('Created content %d for URL %r with slug %r', key.id(), url, content.slug)
        if not content.thumb_url and content.video_url:
            logging.debug('Kicking off thumbnail job for %d', key.id())
            task = taskqueue.Task(url='/_ah/jobs/generate_thumbnail',
                                  params={'content_id': key.id()})
            yield task.add_async(queue_name=config.INTERNAL_QUEUE)
    elif not content:
        logging.debug('Failed to create content from data: %r', {
            'creator': creator,
            'duration': duration,
            'id': content_id,
            'thumb_url': thumb_url,
            'title': title,
            'url': url,
            'video_url': video_url,
            })
    raise ndb.Return((key, content))


def get_or_create_content_from_request(*args, **kwargs):
    return get_or_create_content_from_request_async(*args, **kwargs).get_result()


@ndb.tasklet
def get_or_create_content_from_request_async(prefix='content', get_if_needed=False, **kwargs):
    if prefix and not prefix.endswith('_'):
        prefix = prefix + '_'
    elif not prefix:
        prefix = ''
    content_id = flask_extras.get_parameter(prefix + 'id')
    if content_id:
        try:
            kwargs.setdefault('content_id', int(content_id))
        except:
            raise errors.InvalidArgument('Invalid %sid value' % (prefix,))
    kwargs.setdefault('creator', flask_extras.get_parameter(prefix + 'creator_identifier'))
    kwargs.setdefault('url', flask_extras.get_parameter(prefix + 'url'))
    duration = flask_extras.get_parameter(prefix + 'duration')
    if duration:
        try:
            kwargs.setdefault('duration', int(duration))
        except:
            raise errors.InvalidArgument('Invalid %sduration value' % (prefix,))
    kwargs.setdefault('thumb_url', flask_extras.get_parameter(prefix + 'thumb_url'))
    kwargs.setdefault('title', flask_extras.get_parameter(prefix + 'title'))
    kwargs.setdefault('video_url', flask_extras.get_parameter(prefix + 'video_url'))
    key, content = yield get_or_create_content_async(**kwargs)
    if key and get_if_needed and not content:
        content = yield key.get_async()
    raise ndb.Return((key, content))


def get_streams_data(account):
    if g.api_version < 3:
        return {}
    recents, next_cursor = streams.get_recent(account)
    data = {'streams': recents}
    if g.api_version >= 7:
        data['cursor'] = next_cursor
    return data


@ndb.tasklet
def highres_thumb_url(thumb_url):
    if not thumb_url:
        raise ndb.Return(None)
    if '/hqdefault.jpg' in thumb_url:
        new_thumb_url = thumb_url.replace('/hqdefault.jpg', '/maxresdefault.jpg')
        result = yield ndb.get_context().urlfetch(new_thumb_url, method='HEAD')
        if result.status_code == 200:
            thumb_url = new_thumb_url
    raise ndb.Return(thumb_url)


def normalize_content_urls(url, thumb_url):
    ignored_qs = ['utm_term']
    # YouTube.
    match = re.match(r'https?://(?:(?:www\.|m\.)?youtube\.com/(?:.*?\bv=|embed\/)|youtu\.be/)([^&?#]+)', url)
    if match:
        ignored_qs.append('time_continue')
        vid = match.group(1)
        url = 'https://www.youtube.com/watch?v=%s' % (vid,)
        if not thumb_url:
            thumb_url = 'https://i.ytimg.com/vi/%s/hqdefault.jpg' % (vid,)
        video_url = 'https://www.youtube.com/embed/%s?rel=0&showinfo=0' % (vid,)
    # Google.
    match = re.match(r'https://www\.google(?:\.\w+){1,2}(/search.*)', url)
    if match:
        ignored_qs.extend(['aq', 'biw', 'bih', 'btnG', 'client', 'dpr', 'ei', 'gs_l',
                           'gws_rd', 'hl', 'ie', 'newwindow', 'num', 'oe', 'oq', 'pq',
                           'prmd', 'pws', 'sa', 'sclient', 'source', 'sourceid',
                           'spell', 'ved'])
        url = 'https://www.google.com' + match.group(1)
    # Vimeo.
    match = re.match(r'^https?://(?:player\.)?vimeo\.com/(?:video/|channels/[^/]+/)?(\d+)', url)
    if match:
        vid = match.group(1)
        url = 'https://vimeo.com/%s' % (vid,)
        if not thumb_url:
            thumb_url = 'https://i.vimeocdn.com/video/%s.jpg' % (vid,)
        video_url = 'https://player.vimeo.com/video/%s' % (vid,)
    # Clean up query string items in the URL and remove fragment.
    r = urlparse.urlparse(url)
    items = urlparse.parse_qsl(r.query)
    items = [(k.encode('utf-8'), v.encode('utf-8')) for k, v in items if k not in ignored_qs]
    items = sorted(items, key=lambda i: i[0])
    if items:
        qs = '?' + urllib.urlencode(items)
    else:
        qs = ''
    url = '%s://%s%s%s' % (r.scheme, r.netloc, r.path, qs)
    return url[:1000], thumb_url


def ping_ifttt(account):
    logging.debug('Pinging IFTTT realtime endpoint')
    account_key = models.Account.resolve_key(account)
    headers = {
        'Content-Type': 'application/json',
        'IFTTT-Channel-Key': config.IFTTT_CHANNEL_KEY,
    }
    data = {'data': [{'user_id': str(account_key.id())}]}
    result = urlfetch.fetch(url='https://realtime.ifttt.com/v1/notifications',
                            payload=json.dumps(data),
                            method=urlfetch.POST,
                            headers=headers,
                            follow_redirects=False)
    if result.status_code != 200:
        logging.warning('IFTTT realtime endpoint error: %s', result.content)


def recalculate_content_reaction_counts(account_keys):
    content_futures = []
    for account_key in account_keys:
        q = models.Content.query()
        q = q.filter(models.Content.creator == account_key)
        q = q.filter(models.Content.tags == 'original')
        content_futures.append(q.fetch_async(50))
    account_futures = ndb.get_multi_async(account_keys)
    content_lists = [f.get_result() for f in content_futures]
    accounts_to_put = []
    for i, account_future in enumerate(account_futures):
        account = account_future.get_result()
        if not account:
            logging.error('Account %d could not be found', account_keys[i].id())
            continue
        new_count = sum(c.related_count for c in content_lists[i])
        if new_count == account.content_reaction_count:
            continue
        logging.debug('Content reaction count %d -> %d (based on %d originals) for account %d (@%s)',
                      account.content_reaction_count, new_count,
                      len(content_lists[i]),
                      account.key.id(), account.username)
        account.content_reaction_count = new_count
        accounts_to_put.append(account)
    if accounts_to_put:
        ndb.put_multi(accounts_to_put)
    logging.debug('Updated %d account(s) content reaction counts', len(accounts_to_put))
    return [f.get_result() for f in account_futures]


def set_up_session(handler, participants=None):
    participants = map(accounts.Resolver.parse, participants or [])
    if participants:
        try:
            account_keys = [p.get_or_create_account_key() for p in participants]
            stream = streams.get_or_create(handler.account, account_keys,
                                           reason='session')
            stream.show()
        except:
            logging.debug('Account: %r', handler.account)
            logging.debug('Participants: %r', participants)
            logging.exception('Failed to create initial stream')
    # Return a session object for this user.
    session = handler.create_session(extra_data=get_streams_data(handler.account))
    g.public_options['include_extras'] = True
    g.public_options['view_account'] = session.account
    return session


def upload_and_send(stream):
    extras = flask_extras.get_flag_dict('allow_duplicate', 'export',
                                        'mute_notification', 'persist')
    # Export everything by default.
    extras.setdefault('export', True)
    persist = extras.get('persist', False)
    show_in_recents = flask_extras.get_flag('show_in_recents')
    if show_in_recents is not None:
        extras['show_for_sender'] = show_in_recents
    # Timed transcriptions.
    text_segments = flask_extras.get_parameter('text_segments')
    if text_segments:
        extras['text_segments'] = [models.TextSegment(**s) for s in json.loads(text_segments)]
    # Payload can either be a file + duration or a URL to download.
    if g.api_version >= 29:
        payload = request.files.get('payload')
        url = flask_extras.get_parameter('url')
    else:
        payload = request.files.get('audio')
        url = flask_extras.get_parameter('audio_url')
    if payload:
        try:
            duration = int(flask_extras.get_parameter('duration'))
        except (TypeError, ValueError):
            raise errors.InvalidArgument('Duration should be milliseconds as an int')
        path = files.upload(payload.filename, payload.stream, persist=persist)
    elif url:
        if url.startswith(config.STORAGE_URL_HOST):
            path = '/' + url[len(config.STORAGE_URL_HOST):]
            try:
                duration = int(flask_extras.get_parameter('duration'))
            except (TypeError, ValueError):
                raise errors.InvalidArgument('Duration should be milliseconds as an int')
        else:
            raise errors.InvalidArgument('Cannot use that URL')
    else:
        return False
    # Upload file attachments.
    extras['attachments'] = []
    attachments = request.files.getlist('attachment')
    for attachment in attachments:
        p = files.upload(attachment.filename, attachment.stream, persist=persist)
        extras['attachments'].append(
            models.ChunkAttachment(title=attachment.filename, url=files.storage_url(p)))
    # Add URL attachments.
    attachment_titles = flask_extras.get_parameter_list('attachment_title')
    attachment_urls = flask_extras.get_parameter_list('attachment_url')
    try:
        # Validate URLs.
        parsed_urls = map(urlparse.urlparse, attachment_urls)
    except:
        raise errors.InvalidArgument('Invalid attachment URL provided')
    if not attachment_titles:
        attachment_titles = [u''] * len(attachment_urls)
    for i, title in enumerate(attachment_titles):
        if title: continue
        # Default empty titles to URL hostname.
        attachment_titles[i] = parsed_urls[i].hostname
    if len(attachment_titles) != len(attachment_urls):
        raise errors.InvalidArgument('Attachment title/URL count mismatch')
    for title, url in zip(attachment_titles, attachment_urls):
        extras['attachments'].append(models.ChunkAttachment(title=title, url=url))
    # Support creating public links to chunk.
    if extras.get('export', False):
        custom_content_id = flask_extras.get_parameter('external_content_id')
        if custom_content_id:
            # TODO: WARNING, this can be abused to take over existing shared links!
            if not re.match(r'[a-zA-Z0-9]{21}$', custom_content_id):
                raise errors.InvalidArgument('Custom external_content_id must be 21 base62 digits')
            extras['external_content_id'] = custom_content_id
    client_id, _ = auth.get_client_details()
    extras.setdefault('client_id', client_id)
    stream.send(path, duration, token=flask_extras.get_parameter('chunk_token'), **extras)
    # Ping IFTTT to fetch the new data.
    # TODO: Do this in some nicer way. Maybe on_new_chunk for services?
    if stream.service_id == 'ifttt' and stream.account.username != 'ifttt':
        ping_ifttt(stream.service_owner or stream.account)
    return True
