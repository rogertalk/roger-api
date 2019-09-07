# -*- coding: utf-8 -*-

from collections import defaultdict
from datetime import datetime, timedelta
import json
import logging
import re
import urllib

from google.appengine.api import search, taskqueue, urlfetch
from google.appengine.datastore import datastore_query
from google.appengine.ext import ndb

from flask import Flask, request
import pytz
import twitter

from roger import accounts, config, files, localize, models
from roger import notifs, slack_api, streams, youtube
from roger.apps import utils
from roger_common import convert, errors, events, flask_extras, identifiers, random


app = Flask(__name__)
app.config['DEBUG'] = config.DEVELOPMENT


app_toplevel = ndb.toplevel(app)


twitter_api = twitter.Api(consumer_key=config.TWITTER_CONSUMER_KEY,
                          consumer_secret=config.TWITTER_CONSUMER_SECRET,
                          access_token_key=config.TWITTER_ACCESS_TOKEN_KEY,
                          access_token_secret=config.TWITTER_ACCESS_TOKEN_SECRET)


# Weather bot configuration.
WEATHER_ROOT = u"""<prosody rate="fast">{text}</prosody>"""


# These sentences will be used if the stream is a 1:1 with Weather bot.
WEATHER_SENTENCES_1TO1 = [
    (u"""Today, in {city}, you can expect {weather} weather and a high of {high}.""",),
    (u"""Good morning {name}! Today it will be {weather} in {city} with a high of {high}.""",),
]


# These sentences will be used for groups. The first value in the tuple will be used if
# there is only one person in the city; otherwise, the second value will be used.
WEATHER_SENTENCES_GROUP = [
    (u"""Good morning {name}! Today it will be {weather} in {city} with a high of {high}.""",
     u"""Good morning {city}! Today it will be {weather} in {city} with a high of {high}."""),
    (u"""It's 7 AM for {name}. In {city} you can expect {weather} weather and a high of {high}.""",
     u"""It's 7 AM in {city}, where you can expect {weather} weather and a high of {high}."""),
    (u"""{name} is waking up to a {weather} day in {city} with temperatures of up to {high}.""",
     u"""{city} is waking up to a {weather} day with temperatures of up to {high}."""),
]


WEATHER_TEMP_FORMATS_CELSIUS = [u'{} centigrade', u'{} Celsius', u'{} degrees Celsius']
WEATHER_TEMP_FORMATS_FAHRENHEIT = [u'{} degrees Fahrenheit', u'{} Fahrenheit']
WEATHER_TEMP_FORMATS_SIMPLE = [u'{}', u'{} degrees']


WEATHER_TO_ADJECTIVE = {
    'fog': u'foggy',
    'rain': u'rainy',
    'sleet': u'sleety',
    'snow': u'snowy',
    'wind': u'windy',
}


class CustomNotification(object):
    def __init__(self, notif_type, **kwargs):
        self.data = kwargs
        self.notif_type = notif_type

    def public(self, version=None, **kwargs):
        result = {'api_version': version, 'type': self.notif_type}
        result.update(self.data)
        return result


@app.route('/_ah/jobs/announce_team_join')
def announce_team_join():
    account = models.Account.resolve(request.args['account_id'])
    team_key = models.ServiceTeam.resolve_key((request.args['service_id'], request.args['team_id']))
    q = models.ServiceAuth.query(models.ServiceAuth.service_team == team_key)
    q.map(lambda auth: _notify_team_join(auth, account))
    return ''


@app.route('/_ah/jobs/chat_announce', methods=['POST'])
def chat_announce():
    owner_id = int(request.form['owner_id'])
    channel_id = request.form['channel_id']
    text = request.form['text']
    cursor = datastore_query.Cursor(urlsafe=request.form.get('cursor'))
    owner_key = ndb.Key('Account', owner_id)
    owner_future = owner_key.get_async()
    keys, next_cursor, more = models.AccountFollow.fetch_followers_page(
        owner_key, 50, keys_only=True, start_cursor=cursor)
    owner = owner_future.get_result()
    futures = []
    if more:
        task = taskqueue.Task(
            url='/_ah/jobs/chat_announce',
            params={'owner_id': owner_id,
                    'channel_id': channel_id,
                    'text': text,
                    'cursor': next_cursor.urlsafe()},
            retry_options=taskqueue.TaskRetryOptions(task_retry_limit=0))
        futures.append(_add_task_async(task, queue_name=config.INTERNAL_QUEUE))
    logging.debug('Notifying %d/%d followers that %d is on chat: %r',
        len(keys), owner.follower_count, owner_id, text)
    futures.extend(_notify_follower_chat_async(k, owner, channel_id, text) for k in keys)
    _wait_all(futures)
    return ''


@app.route('/_ah/jobs/content_became_public', methods=['POST'])
def content_became_public():
    # Extract all parameters from the request.
    content_id = int(request.form['content_id'])
    content_key = ndb.Key('Content', content_id)
    creator_id = int(request.form['creator_id'])
    creator_key = ndb.Key('Account', creator_id)
    original_id = request.form['original_id']
    original_id = int(original_id) if original_id else None
    content, creator = ndb.get_multi([content_key, creator_key])
    if not content or not creator:
        logging.error('Failed to load content %d or creator %d', content_id, creator_id)
        return ''
    futures = []
    # Related content counter:
    if original_id:
        original_key = ndb.Key('Content', original_id)
        original_future = _increment_content_reaction_count_async(original_key, creator)
        futures.append(original_future)
    else:
        original_key = None
        original_future = None
    # Follower notifications:
    if creator.follower_count > 0:
        task = taskqueue.Task(
            url='/_ah/jobs/content_followers',
            countdown=30,
            params={'content_id': content_id,
                    'creator_id': creator_id},
            retry_options=taskqueue.TaskRetryOptions(task_retry_limit=0))
        futures.append(_add_task_async(task, queue_name=config.INTERNAL_QUEUE))
    # Content request notifications:
    if original_id:
        task = taskqueue.Task(
            url='/_ah/jobs/content_requests',
            countdown=30,
            params={'content_id': content_id,
                    'creator_id': creator_id,
                    'original_id': original_id})
        futures.append(_add_task_async(task, queue_name=config.INTERNAL_QUEUE))
    # Mentions:
    futures.append(_notify_content_mentions_async(creator, content))
    # Fetch original since it's needed later.
    if original_future:
        o, first = original_future.get_result()
    else:
        o, first = None, False
    # Streaks.
    task = taskqueue.Task(url='/_ah/jobs/content_streak', params={
        'creator_id': creator_id,
        'first': 'true' if first else 'false'})
    futures.append(_add_task_async(task, queue_name=config.INTERNAL_QUEUE))
    if o and first:
        # Report newfound content.
        event = events.ContentFirstV1(
            account_key=creator_key,
            content_id=o.key.id(),
            tags=content.tags)
        futures.append(event.report_async())
    if o and o.creator and o.creator.id() != config.ANONYMOUS_ID:
        o_creator_future = o.creator.get_async()
        futures.append(o_creator_future)
    else:
        o_creator_future = None
    # Start secondary level tasks that aren't as important.
    aux_futures = []
    if not creator.quality_has_been_set:
        aux_futures.append(_send_review_to_slack_async(content, creator))
    aux_futures.append(_ensure_content_has_thumbnail_async(content))
    # Wait for the futures to be completed.
    _wait_all(futures)
    # (Re-)index original content with its new rank.
    if o:
        try:
            o_creator = o_creator_future.get_result() if o_creator_future else None
            _put_content_in_search_index(o_creator, o)
        except:
            logging.exception('Failed to add content to search index')
    # Check status of extra jobs.
    try:
        _wait_all(aux_futures)
    except errors.ExternalError as e:
        logging.error('Failed to finish aux futures (%s)', e)
    except:
        logging.exception('Failed to finish aux futures')
    return ''


@app.route('/_ah/jobs/content_comment_extras', methods=['POST'])
def content_comment_extras():
    # Extract all parameters from the request.
    content_id = int(request.form['content_id'])
    content_key = ndb.Key('Content', content_id)
    comment_id = request.form['comment_id']
    comment_key = ndb.Key('ContentComment', comment_id, parent=content_key)
    commenter_id = int(request.form['commenter_id'])
    commenter_key = ndb.Key('Account', commenter_id)
    creator_id = int(request.form['creator_id'])
    creator_key = ndb.Key('Account', creator_id)
    mentions = request.form['mentions'].split(',')
    if len(mentions) == 1 and not mentions[0]:
        mentions = []
    # Load relevant entities asynchronously.
    account_keys_futures = [models.Account.resolve_key_async(m) for m in mentions]
    commenter_future = commenter_key.get_async()
    comment_future = comment_key.get_async()
    content_future = content_key.get_async()
    commenter = commenter_future.get_result()
    comment = comment_future.get_result()
    if not comment:
        logging.warning('Failed to find comment with key %r', comment_key)
        return ''
    # Build a set of keys for the accounts to notify.
    interested_keys = set()
    # All mentioned users.
    for f in account_keys_futures:
        try:
            account_key = f.get_result()
        except:
            logging.exception('Failed to get key for a mentioned account.')
            continue
        interested_keys.add(account_key)
    # But not the creators of the comment or the content.
    interested_keys.discard(commenter_key)
    interested_keys.discard(creator_key)
    interested_keys.discard(None)
    # Notify away!
    content = content_future.get_result()
    futures = []
    for account_key in interested_keys:
        hub = notifs.Hub(account_key)
        futures.append(hub.emit_async(notifs.ON_CONTENT_COMMENT,
            comment=comment,
            commenter=commenter,
            content=content))
    for f in futures:
        try:
            f.get_result()
        except:
            logging.exception('Failed to notify an account.')
    return ''


@app.route('/_ah/jobs/content_followers', methods=['POST'])
def content_followers():
    creator_id = int(request.form['creator_id'])
    content_id = int(request.form['content_id'])
    cursor = datastore_query.Cursor(urlsafe=request.form.get('cursor'))
    creator_key = ndb.Key('Account', creator_id)
    creator_future = creator_key.get_async()
    content_key = ndb.Key('Content', content_id)
    content_future = content_key.get_async()
    creator, content = creator_future.get_result(), content_future.get_result()
    if not content.is_public:
        logging.debug('Content %d by %d is no longer public', content_id, creator_id)
        return ''
    futures = []
    keys, next_cursor, more = models.AccountFollow.fetch_followers_page(
        creator_key, 100, keys_only=True, start_cursor=cursor)
    if more:
        task = taskqueue.Task(
            url='/_ah/jobs/content_followers',
            params={'content_id': content_id,
                    'creator_id': creator_id,
                    'cursor': next_cursor.urlsafe()},
            retry_options=taskqueue.TaskRetryOptions(task_retry_limit=0))
        futures.append(_add_task_async(task, queue_name=config.INTERNAL_QUEUE))
    logging.debug('Notifying %d/%d followers about content %d by %d',
        len(keys), creator.follower_count, content_id, creator_id)
    futures.extend(_notify_follower_content_async(k, creator, content) for k in keys)
    _wait_all(futures)
    return ''


@app.route('/_ah/jobs/content_request_send', methods=['POST'])
def content_request_send():
    requester_id = int(request.form['requester_id'])
    account_ids = map(int, request.form.getlist('account_id'))
    comment = (request.form.get('comment') or u'').strip() or None
    content_id = int(request.form['content_id'])
    f = _send_content_requests_async(requester_id, account_ids, content_id,
                                     comment=comment)
    if all(f.get_result()):
        logging.debug('Sent requests for %d from %d to %r',
                      content_id, requester_id, account_ids)
    else:
        logging.error('Failed to send one or more requests for %d from %d to %r',
                      content_id, requester_id, account_ids)
    return ''


@app.route('/_ah/jobs/content_requests', methods=['POST'])
def content_requests():
    creator_id = int(request.form['creator_id'])
    original_id = int(request.form['original_id'])
    content_id = int(request.form['content_id'])
    creator_key = ndb.Key('Account', creator_id)
    creator_future = creator_key.get_async()
    content_key = ndb.Key('Content', content_id)
    content_future = content_key.get_async()
    q = models.ContentRequest.query(ancestor=creator_key)
    q = q.filter(models.ContentRequest.content == ndb.Key('Content', original_id))
    def handle_request(cr):
        logging.debug('Handling content request (content id: %d) from %d to %d',
            original_id, cr.requested_by.id(), creator_id)
        return _notify_requester_async(cr, creator_future, content_future)
    q.map(handle_request)
    return ''


@app.route('/_ah/jobs/content_streak', methods=['POST'])
def content_streak():
    creator_id = int(request.form['creator_id'])
    creator_key = ndb.Key('Account', creator_id)
    first = request.form.get('first') == 'true'
    # This also updates the content count of the account.
    account, before, after = _update_streak_async(creator_key, first).get_result()
    logging.debug('%s (%d) has posted %d videos total and is on a %d-day streak (was %d)',
        account.username, account.key.id(),
        account.content_count,
        after, before)
    if after > 1 and after > before:
        hub = notifs.Hub(account)
        hub.emit(notifs.ON_STREAK, days=after)
    return ''


@app.route('/_ah/jobs/content_tweet', methods=['POST'])
def content_tweet():
    creator_id = int(request.form['creator_id'])
    original_id = request.form['original_id']
    original_id = int(original_id) if original_id else None
    content_id = int(request.form['content_id'])
    creator_key = ndb.Key('Account', creator_id)
    content_key = ndb.Key('Content', content_id)
    if original_id:
        original_key = ndb.Key('Content', original_id)
        creator, content, original = ndb.get_multi([creator_key, content_key, original_key])
        if original:
            if original.creator_twitter:
                original_twitters = original.creator_twitter.split(',')
                original_twitters = map(lambda h: h.strip().lstrip('@'), original_twitters)
            else:
                original_creator = original.creator.get()
                if original_creator and original_creator.properties:
                    handle = original_creator.properties.get('twitter')
                    original_twitters = [handle] if handle else []
                else:
                    original_twitters = []
        else:
            original_id = None
            original_key = None
            original_twitters = []
    else:
        creator, content = ndb.get_multi([creator_key, content_key])
        original = None
        original_twitters = []
    if not content or not content.is_public:
        return ''
    web_url = content.web_url
    if not web_url:
        return ''
    pieces = []
    if original and original.title:
        pieces.append(original.title)
        pieces.append(u'REACTION')
    elif content.title:
        pieces.append(content.title)
    else:
        pieces.append(u'😂 REACTION')
    if pieces[0].startswith('@'):
        pieces[0] = '.' + pieces[0]
    if creator:
        handle = creator.properties.get('twitter')
        if handle:
            pieces.append(u'by @' + handle)
    tags = []
    if content.title:
        # TODO: Emoji support.
        tags.extend(re.findall(r'#\w+', content.title))
    # Allow tags to take up to 50 characters.
    tags_length = 0
    while tags and tags_length < 50:
        tag = tags.pop(0)
        if tag in pieces[0]:
            continue
        new_tags_length = tags_length + 1 + len(tag)
        if new_tags_length <= 50:
            pieces.append(tag)
            tags_length = new_tags_length
    # Measure tweet length and shorten title if necessary.
    tweet_length = len(u' '.join(pieces)) + 24  # All links take up 23 characters.
    if tweet_length > 140:
        keep = len(pieces[0]) - (tweet_length - 140) - 1
        if keep > 0:
            pieces[0] = pieces[0][:keep].strip() + u'…'
        else:
            del pieces[0]
    pieces.append(web_url)
    tweet = u' '.join(pieces)
    logging.debug('Tweet: %s', tweet)
    if '@' not in tweet:
        # Don't tweet tweets without mentions.
        logging.debug('(Skipped)')
        return ''
    if config.PRODUCTION:
        twitter_api.PostUpdate(tweet, verify_status_length=False)
    return ''


@app.route('/_ah/jobs/content_was_deleted', methods=['POST'])
def content_was_deleted():
    # Extract all parameters from the request.
    creator_id = int(request.form['creator_id'])
    original_id = request.form['original_id']
    original_id = int(original_id) if original_id else None
    content_id = int(request.form['content_id'])
    futures = []
    if original_id:
        original_key = ndb.Key('Content', original_id)
        futures.append(_recount_content_reactions_async(original_key))
    account_key = ndb.Key('Account', creator_id)
    futures.append(_recount_account_reactions_async(account_key))
    _wait_all(futures)
    return ''


@app.route('/_ah/jobs/recount_originals', methods=['POST'])
def recount_originals():
    account_id = request.form['account_id']
    account_id = int(account_id) if account_id else None
    if not account_id:
        return
    account_key = ndb.Key('Account', account_id)
    utils.recalculate_content_reaction_counts([account_key])
    return ''


@app.route('/_ah/jobs/recount_reactions', methods=['POST'])
def recount_reactions():
    original_id = request.form['original_id']
    original_id = int(original_id) if original_id else None
    if not original_id:
        return
    original_key = ndb.Key('Content', original_id)
    _recount_content_reactions_async(original_key).get_result()
    return ''


@app.route('/_ah/jobs/delete_chunks')
def delete_chunks():
    cursor = datastore_query.Cursor(urlsafe=request.args.get('cursor'))
    delete_before = int(request.args['delete_before'])
    threshold = datetime.utcfromtimestamp(delete_before)
    query = models.Chunk.query(models.Chunk.end < threshold,
                               models.Chunk.persist == False)
    keys, next_cursor, more = query.fetch_page(1000, keys_only=True, start_cursor=cursor)
    if next_cursor and more:
        # There are still more chunks to delete after this batch.
        taskqueue.add(
            method='GET',
            url='/_ah/jobs/delete_chunks',
            params={'cursor': next_cursor.urlsafe(), 'delete_before': delete_before},
            queue_name=config.DELETE_CHUNKS_QUEUE_NAME)
    # Delete the chunks.
    ndb.delete_multi(keys)
    logging.info('Deleted %d expired chunks', len(keys))
    return ''


@app.route('/_ah/jobs/export_content', methods=['POST'])
def export_content():
    account_key = models.Account.resolve_key(request.form['account_id'])
    client_id = request.form['client_id'] or None
    if client_id in ('fika', 'fikaio'):
        app = 'fika'
    elif client_id == 'reactioncam':
        app = 'reactioncam'
    else:
        logging.warning('Unsupported client %r', client_id)
        return ''
    destination_id = request.form['destination']
    if destination_id:
        service_key, team_key, resource = models.Service.parse_identifier(destination_id)
        if service_key.id() != 'slack':
            # TODO: Support more services?
            logging.error('Unsupported service %s', service_key.id())
            return ''
        auth_key = models.ServiceAuth.resolve_key((account_key, service_key, team_key))
        auth, team = ndb.get_multi([auth_key, team_key])
        if not auth:
            logging.warning('Failed to get %s credentials for %d',
                            service_key.id(), account_key.id())
            return ''
    else:
        auth, team, resource = (None, None, None)
    # Create a reference to this content, used for presentation.
    text_segments = json.loads(request.form['text']) if request.form['text'] else []
    props = {
        'account_image_url': request.form['account_image_url'],
        'account_name': request.form['account_name'],
        'stream_image_url': request.form['stream_image_url'],
        'stream_title': request.form['stream_title'],
        'text': text_segments,
    }
    unix_timestamp_ms = int(request.form['timestamp'])
    timestamp = convert.from_unix_timestamp_ms(unix_timestamp_ms)
    stream_key = ndb.Key('Stream', int(request.form['stream_id']))
    chunk_key = ndb.Key('Chunk', int(request.form['chunk_id']), parent=stream_key)
    stream = streams.Stream(stream_key.get())
    prev_content_key = None
    if stream.title != streams.SHARE_STREAM_TITLE:
        for p, c in zip(stream.chunks, stream.chunks[1:]):
            if c.chunk_id == chunk_key.id():
                if p.external_content_id:
                    prev_content_key = ndb.Key('ExportedContent', p.external_content_id)
                break
    attachments = json.loads(request.form['attachments'])
    content = models.ExportedContent.create(
        account=account_key,
        attachments=[models.ChunkAttachment(**a) for a in attachments],
        auth=auth.key if auth else None,
        chunk=chunk_key,
        content_id=request.form['content_id'] or None,
        destination_id=destination_id,
        duration=int(request.form['duration']),
        prev_content=prev_content_key,
        properties=props,
        timestamp=timestamp,
        url=request.form.get('url'))
    if prev_content_key:
        prev_content = prev_content_key.get()
        if prev_content:
            prev_content.next_content = content.key
            prev_content.put()
        else:
            logging.error('Could not find content before %s (looked for %s)',
                          content.key.id(), prev_content_key.id())
    # TODO: Change domain based on client_id.
    domain = 'watch.fika.io'
    url = 'https://%s/-/%s' % (domain, content.key.id(),)
    # Kick off requests to make sure the shared page and .gif get cached in the CDN.
    context = ndb.get_context()
    context.urlfetch(url + '.gif')
    context.urlfetch(url + '.jpg')
    # Send notification to the first email of every participant.
    sender = stream.lookup_account(account_key)
    transcript = ' '.join(s['text'] for s in text_segments)
    time = timestamp.replace(tzinfo=pytz.utc)
    for a in stream.get_accounts(exclude_account=account_key):
        email = a.first_email
        if not email:
            logging.debug('No email found for %s (%d)', a.display_name, a.key.id())
            continue
        if stream.title:
            # The sentence will be something like "[#general has an update] on fika.io".
            destination_sentence = '%s has an update' % (stream.title,)
        elif len(stream.participants) != 2:
            # The sentence will be something like "[Jane Doe, John Smith has an update] on fika.io".
            s = stream.for_participant(a)
            destination_sentence = '%s has an update' % (s.presentation_title,)
        else:
            # The sentence will be something like "[Jane Doe sent you a video] on fika.io".
            destination_sentence = '%s sent you a video' % (sender.display_name,)
        if a.location_info:
            tz = pytz.timezone(a.location_info.timezone)
            local_time = time.astimezone(tz).strftime('%-I:%M:%S %p')
        else:
            # Let's just default to New York time in this rare case.
            tz = pytz.timezone('America/New_York')
            local_time = time.astimezone(tz).strftime('%-I:%M:%S %p %Z')
        localize.send_email(
            app,
            'unplayed_chunk_transcript' if transcript else 'unplayed_chunk',
            to=email,
            sender=sender.first_email,
            sender_name=sender.display_name,
            cc_sender=False,
            destination_sentence=destination_sentence,
            thumb=url + '.gif',
            time=local_time,
            transcript=transcript,
            watch_url=url)
    if not auth:
        context.urlfetch(url)
        return ''
    logging.debug('%s posted to %s', props['account_name'], props['stream_title'])
    # From here on we assume export to Slack chat.
    slack_api.chat(resource, url, auth.access_token)
    return ''


@app.route('/_ah/jobs/fix_content', methods=['POST'])
def fix_content():
    try:
        content_id = int(request.form['content_id'])
    except:
        logging.warning('Invalid content_id %r', request.form.get('content_id'))
        return ''
    content = models.Content.get_by_id(content_id)
    if not content:
        logging.warning('Could not find content with id %d', content_id)
        return ''
    if not content.url:
        logging.debug('Content has no legacy URL set')
        return ''
    url = re.sub(r'^https?://(?:(?:www\.|m\.)?youtube\.com/.*?\bv=|youtu\.be/)([^&?#]+).*$',
                 'https://www.youtube.com/watch?v=\\1', content.url)
    original = models.Content.query(models.Content.original_url == url).get()
    if not original:
        logging.warning('Could not find original content for URL %s', url)
        return ''
    content.related_to = original.key
    content.url = None
    content.put()
    return ''


@app.route('/_ah/jobs/forget_youtube_views_updated', methods=['POST'])
def forget_youtube_views_updated():
    try:
        content_id = int(request.form['content_id'])
    except:
        return 'Invalid content id.', 400
    content = models.Content.get_by_id(content_id)
    if not content:
        return 'Content not found.', 404
    content.youtube_views_updated = None
    content.put()
    return ''


@app.route('/_ah/jobs/generate_thumbnail', methods=['POST'])
def generate_thumbnail():
    content_id = int(request.form['content_id'])
    content_key = ndb.Key('Content', content_id)
    thumb_future = _ensure_content_has_thumbnail_async(content_key.get())
    try:
        thumb_future.get_result()
    except errors.ExternalError as e:
        logging.error('Failed to generate thumbnail (%s)', e)
    except:
        logging.exception('Failed to generate thumbnail')
    return ''


@app.route('/_ah/jobs/import_slack_channels')
def import_slack_channels():
    account, auth = _get_auth()
    if not account or not auth:
        return ''
    data = slack_api.get_channel_list(auth.access_token)
    if not data:
        return ''
    for channel in data['channels']:
        if not channel['is_member'] or channel['num_members'] > 200:
            # Don't import huge channels or channels the user is not participating in.
            continue
        _create_or_join_channel(account, auth, channel)
    return ''


@app.route('/_ah/jobs/import_slack_groups')
def import_slack_groups():
    account, auth = _get_auth()
    if not account or not auth:
        return ''
    data = slack_api.get_group_list(auth.access_token)
    if not data:
        return ''
    for group in data['groups']:
        if group['is_mpim']:
            # Don't import private group chats (only private channels).
            continue
        _create_or_join_channel(account, auth, group)
    return ''


@app.route('/_ah/jobs/index_content', methods=['POST'])
def index_content():
    content_id = request.form.get('content_id')
    try:
        content_id = int(content_id)
    except:
        logging.error('Invalid content id: %r', content_id)
        return ''
    content = models.Content.get_by_id(content_id)
    if not content:
        logging.error('Content %d does not exist', content_id)
        return ''
    logging.debug('Putting content %d in index', content_id)
    creator = content.creator.get() if content.creator else None
    _put_content_in_search_index(creator, content)
    return ''


@app.route('/_ah/jobs/notify_batch', methods=['POST'])
def notify_batch():
    app = request.form.get('app')
    if not app:
        logging.error('Missing app')
        return ''
    env = request.form.get('env', '')
    title = request.form.get('title')
    text = request.form.get('text')
    if not text:
        logging.error('Missing text')
        return ''
    tokens = request.form.getlist('token')
    if not tokens:
        return ''
    headers = {'Content-Type': 'application/json'}
    payload_data = {
        'app': app,
        'data': {
            'aps': {
                'alert': {
                    'body': text,
                    'title': title,
                },
                'sound': 'default',
            },
        },
        'device_token': '{{TOKEN}}',
        'environment': env,
    }
    payload_template = json.dumps(payload_data)
    payload = '\n'.join(payload_template.replace('{{TOKEN}}', t) for t in tokens)
    try:
        result = urlfetch.fetch(
            url=config.SERVICE_PUSH,
            method=urlfetch.POST,
            payload=payload,
            headers=headers,
            follow_redirects=False,
            deadline=60)
    except:
        logging.exception('Failed to call push service')
        return '', 502
    if result.status_code == 200:
        logging.info(result.content)
    else:
        logging.warning(payload)
        logging.warning(result.content)
    return ''


@app.route('/_ah/jobs/set_up_new_service', methods=['GET'])
def set_up_new_service():
    # Extract all parameters from the request.
    account_id = int(request.args['account_id'])
    handler = accounts.get_handler(account_id)
    notify = flask_extras.get_flag('notify')
    service_key = models.Service.resolve_key(request.args['service_id'])
    team_id = request.args['team_id'] or None
    if team_id:
        team_key = models.ServiceTeam.resolve_key((service_key, team_id))
    else:
        team_key = None
    auth_key = models.ServiceAuth.resolve_key((account_id, service_key, team_key))
    client_id = request.args['client_id']
    resource = request.args['resource']
    # Perform team specific logic.
    if not team_key:
        # No team joined, so don't do anything.
        return ''
    if service_key.id() == 'email':
        # Don't run any team logic on public email domains.
        if team_key.id() in config.PUBLIC_EMAIL_DOMAINS:
            logging.debug('Not setting up public email domain %s', team_key.id())
            return ''
    if not notify:
        logging.debug('Not notifying team join because notify=False')
        return ''
    if client_id == 'fikaio':
        # Don't notify about users that log in on the fika.io website.
        # TODO: Don't hardcode it like this.
        logging.debug('Not notifying team join for fika.io login')
        return ''
    # Also kick off a job which notifies all team members.
    logging.debug('Queueing job to announce team join')
    params = request.args.to_dict()
    taskqueue.add(method='GET', url='/_ah/jobs/announce_team_join', params=params,
                  queue_name=config.SERVICE_QUEUE_NAME)
    return ''


@app.route('/_ah/jobs/set_quality', methods=['POST'])
def set_quality():
    account_key = models.Account.resolve_key(request.form['account_id'])
    quality = int(request.form['quality'])
    account = _set_account_quality_async(account_key, quality).get_result()
    if account.publish:
        _publish_recent_content_async(account_key, quality).get_result()
    return ''


@app.route('/_ah/jobs/track_login', methods=['POST'])
def track_login():
    account_key = models.Account.resolve_key(request.form['account_id'])
    account, auths_and_teams = _get_tracking_data(account_key)
    if account.created:
        account_is_new = datetime.utcnow() - account.created < timedelta(minutes=5)
    else:
        account_is_new = False
    client_id = request.form['client_id']
    # Send fika.io welcome message.
    if client_id == 'fika' and not account.disable_welcome:
        try:
            fikaio = models.Account.resolve(5751242904043520)
            stream = streams.get_or_create(fikaio, [account], reason='welcome')
            # TODO: Fill out text_segments.
            text_segments = []
            stream.send(
                '/roger-api-persistent/80b7b2e4b25b33f49864c86eeba7dfa6c6529197d8d3f28b7607618a1301e7f3.mp4',
                77000,
                text_segments=text_segments)
        except errors.ForbiddenAction:
            # The welcome message was already sent before.
            pass
    # Schedule a chain of welcome emails to the user.
    auth_identifier = request.form.get('auth_identifier')
    if auth_identifier:
        email = identifiers.email(auth_identifier)
        if client_id == 'fika' and email:
            logging.debug('Skipping welcome emails for %s (disabled)', email)
            #localize.schedule_welcome_emails(account, email)
    # Set up new reaction.cam accounts.
    if client_id == 'reactioncam' and account_is_new:
        futures = []
        fb_token = request.form.get('fb_token')
        if fb_token:
            # Notify friends already on the app.
            futures.append(_notify_fb_friends_async(account, fb_token))
        futures.append(_schedule_requests_async(account))
        _wait_all(futures)
    # Don't report 47center.com logins to Slack.
    for _, t in auths_and_teams:
        if not t:
            continue
        if t.key.id() == '47center.com' and t.key.parent().id() == 'email':
            return ''
    # Announce logins in Slack.
    pretty_clients = {'fika': 'fika.io', 'reactioncam': 'reaction.cam'}
    team_links = ', '.join(slack_api.admin(t) for _, t in auths_and_teams if t)
    text = '%s (%s) logged in to %s.' % (
        slack_api.admin(account),
        team_links or 'no team',
        pretty_clients.get(client_id, client_id))
    slack_api.message(channel='#review', text=text, hook_id='fika')
    return ''


@app.route('/_ah/jobs/update_content_request_entries', methods=['POST'])
def update_content_request_entries():
    request_id = int(flask_extras.get_parameter('request_id'))
    wallet_id = flask_extras.get_parameter('wallet_id')
    assert wallet_id
    wallet_owner_id = int(flask_extras.get_parameter('wallet_owner_id'))
    request_key = ndb.Key('ContentRequestPublic', request_id)
    q = models.ContentRequestPublicEntry.query()
    q = q.filter(models.ContentRequestPublicEntry.request == request_key)
    q = q.filter(models.ContentRequestPublicEntry.status == 'active')
    q = q.order(-models.ContentRequestPublicEntry.created)
    futures = []
    tasks = []
    job_count = 0
    delay = 10  # 10 seconds to allow YouTube update to finish.
    content_ids = []
    for entry in q:
        task = taskqueue.Task(
            countdown=delay,
            url='/_ah/jobs/update_content_request_entry',
            params={
                'account_id': str(entry.account.id()),
                'content_id': str(entry.content.id()),
                'request_id': str(entry.request.id()),
                'wallet_id': wallet_id,
                'wallet_owner_id': str(wallet_owner_id),
                'youtube_id': entry.youtube_id,
            },
            retry_options=taskqueue.TaskRetryOptions(task_retry_limit=0))
        tasks.append(task)
        if len(tasks) == 100:
            futures.append(_add_task_list_async(tasks, queue_name=config.INTERNAL_QUEUE))
            tasks = []
        delay += 1
        job_count += 1
        content_ids.append(entry.content.id())
    del q, task, tasks
    if content_ids:
        # Update YouTube views on all content ids.
        futures.append(_update_youtube_views_async(content_ids))
        job_count += 1
    del content_ids
    _wait_all(futures)
    logging.debug('Scheduled %d update job(s) for request %d', job_count, request_id)
    return ''


@app.route('/_ah/jobs/update_content_request_entry', methods=['POST'])
def update_content_request_entry():
    account_id = int(flask_extras.get_parameter('account_id'))
    account_key = ndb.Key('Account', account_id)
    content_id = int(flask_extras.get_parameter('content_id'))
    content_key = ndb.Key('Content', content_id)
    request_id = int(flask_extras.get_parameter('request_id'))
    entry_key = ndb.Key('ContentRequestPublicEntry', '%d.%d' % (request_id, account_id))
    wallet_id = flask_extras.get_parameter('wallet_id')
    wallet_key = models.Wallet.key_from_id(wallet_id)
    wallet_owner_id = int(flask_extras.get_parameter('wallet_owner_id'))
    wallet_owner_key = ndb.Key('Account', wallet_owner_id)
    youtube_id = flask_extras.get_parameter('youtube_id')
    assert youtube_id
    force_update = flask_extras.get_flag('force_update')
    logging.debug('Checking rewards for account %d on request %d (content %d)',
        account_id, request_id, content_id)
    # Load the content outside of a transaction.
    content = content_key.get()
    assert content
    last_update = content.youtube_views_updated
    if not content.youtube_broken and last_update and not force_update:
        skip = (datetime.utcnow() - last_update) > timedelta(minutes=20)
    else:
        skip = False
    if skip:
        # Assume that there is no new info to deal with to save ourselves a transaction.
        logging.debug('YouTube views did not change recently, skipping update')
        return ''
    future = models.ContentRequestPublicEntry.reward_async(entry_key, content, wallet_owner_key, wallet_key)
    amount = future.get_result()
    if amount:
        # Tell app to update if user is looking at the request.
        hub = notifs.Hub(account_key)
        hub.emit(notifs.ON_PUBLIC_REQUEST_UPDATE, request_id=request_id)
    return ''


@app.route('/_ah/jobs/update_youtube_channel', methods=['POST'])
def update_youtube_subs():
    account_id = int(request.form['account_id'])
    account_key = ndb.Key('Account', account_id)
    auth = models.ServiceAuth.resolve_key((account_key, 'youtube', None)).get()
    if not auth or not auth.refresh_token:
        logging.warning('Account %d is not connected to YouTube', account_id)
        return ''
    try:
        channels = youtube.get_channels_async(auth.refresh_token).get_result()
    except:
        logging.exception('Failed to get YouTube channels for account %d', account_id)
        return ''
    if not channels:
        logging.warning('Account %d does not have any channels', account_id)
        return ''
    _set_account_youtube_channel_async(account_key, channels[0]).get_result()
    return ''


@app.route('/_ah/jobs/update_youtube_views', methods=['POST'])
def update_youtube_views():
    try:
        content_ids = map(int, request.form.getlist('content_id'))
    except:
        logging.error('Got one or more invalid content ids')
        return ''
    if not content_ids:
        return ''
    _update_youtube_views_async(content_ids).get_result()
    return ''


@app.route('/_ah/jobs/update_youtube_views_batched', methods=['POST'])
def update_youtube_views_batched():
    params = {}
    repair = flask_extras.get_flag('repair') or False
    if repair:
        carry = int(request.form.get('carry') or '0')
        params['repair'] = 'true'
    creator_id = request.form.get('creator_id')
    if creator_id:
        params['creator_id'] = creator_id
        creator_id = int(creator_id)
        creator_key = ndb.Key('Account', creator_id)
    else:
        creator_key = None
    original_id = request.form.get('original_id')
    if original_id:
        params['original_id'] = original_id
        original_id = int(original_id)
        original_key = ndb.Key('Content', original_id)
    else:
        original_key = None
    if (creator_id and original_id) or not (creator_id or original_id):
        logging.error('Invalid parameter combination: %r', params)
        return ''
    q = models.Content.query()
    if creator_id:
        q = q.filter(models.Content.creator == creator_key)
        logging.debug('Looking for content by account %d', creator_id)
    if original_id:
        q = q.filter(models.Content.related_to == original_key)
        logging.debug('Looking for content related to content %d', original_id)
    q = q.filter(models.Content.tags == 'reaction')
    q = q.order(-models.Content.created)
    delay = 0
    page_size = 1000 if repair else 200
    cursor = datastore_query.Cursor(urlsafe=request.form.get('cursor'))
    content_list, next_cursor, more = q.fetch_page(page_size, start_cursor=cursor)
    futures = []
    if repair:
        views = sum((c.youtube_views or 0) for c in content_list)
        carry += views
        params['carry'] = str(carry)
        logging.debug('Counted %d YouTube views for %d content entities', views, len(content_list))
    else:
        content_ids = [c.key.id() for c in content_list if c.youtube_id]
        chunk_size = 15
        for i in xrange(0, len(content_ids), chunk_size):
            task = taskqueue.Task(
                countdown=delay,
                url='/_ah/jobs/update_youtube_views',
                params={'content_id': map(str, content_ids[i:i+chunk_size])})
            futures.append(_add_task_async(task, queue_name=config.INTERNAL_QUEUE))
            delay += 1
        if content_ids:
            logging.debug('Scheduled %d job(s) for updating %d Content entities',
                          len(content_ids) // chunk_size + 1, len(content_ids))
    if more:
        params['cursor'] = next_cursor.urlsafe()
        task = taskqueue.Task(
            countdown=delay,
            url='/_ah/jobs/update_youtube_views_batched',
            params=params,
            retry_options=taskqueue.TaskRetryOptions(task_retry_limit=0))
        futures.append(_add_task_async(task, queue_name=config.INTERNAL_QUEUE))
        logging.debug('Scheduled next page of batch update with parameters: %r', params)
    elif repair:
        # Final page for repair job.
        entity = (creator_key or original_key).get()
        entity.youtube_reaction_views = carry
        entity.youtube_reaction_views_updated = datetime.utcnow()
        futures.append(entity.put_async())
        logging.debug('Repaired aggregate YouTube views of %r to %d', entity.key, carry)
    _wait_all(futures)
    return ''


@ndb.tasklet
def _add_task_async(task, **kwargs):
    yield task.add_async(**kwargs)


@ndb.tasklet
def _add_task_list_async(tasks, queue_name=None):
    queue = taskqueue.Queue(queue_name) if queue_name else taskqueue.Queue()
    yield queue.add_async(tasks)


def _create_or_join_channel(account, auth, channel):
    content_id = auth.build_identifier(channel['id'])
    return streams.join_service_content(
        account, content_id,
        service_members=channel['members'],
        title='#%s' % (channel['name'],))


@ndb.tasklet
def _ensure_content_has_thumbnail_async(content):
    if not content.video_url:
        logging.debug('No thumb requested (missing video_url)')
        return
    qs = urllib.urlencode({'url': content.video_url})
    yield _set_content_thumbnail_async(content.key, config.SERVICE_THUMBNAIL + '?' + qs)


def _get_auth():
    account_key = models.Account.resolve_key(request.args.get('account_id'))
    if not account_key:
        logging.warning('Invalid account %r', request.args.get('account_id'))
        return None, None
    auth_key = models.ServiceAuth.resolve_key((account_key, 'slack', request.args.get('team_id')))
    account, auth = ndb.get_multi([account_key, auth_key])
    if not account:
        logging.warning('Failed to get account for %d', account_key.id())
    if not auth:
        logging.warning('Failed to get Slack credentials for %d', account_key.id())
    return account, auth


@ndb.tasklet
def _get_auth_team_async(auth):
    if not auth.service_team:
        raise ndb.Return((auth, None))
    team = yield auth.service_team.get_async()
    raise ndb.Return((auth, team))


@ndb.synctasklet
def _get_tracking_data(key):
    q = models.ServiceAuth.query(ancestor=key)
    account, auths_and_teams = yield key.get_async(), q.map_async(_get_auth_team_async)
    raise ndb.Return((account, auths_and_teams))


@ndb.transactional_tasklet
def _increment_content_reaction_count_async(content_key, related_creator):
    content = yield content_key.get_async()
    first = content.add_related_count(related_creator)
    logging.debug('Updated content %d related count to %d',
                  content_key.id(), content.related_count)
    yield content.put_async()
    raise ndb.Return((content, first))


@ndb.tasklet
def _notify_content_mentions_async(creator, content):
    if not content.title:
        return
    mentions = identifiers.find_mentions(content.title)
    if not mentions:
        return
    account_keys = yield tuple(models.Account.resolve_key_async(m) for m in mentions)
    futures = []
    for account_key in account_keys:
        if not account_key or account_key == content.creator:
            continue
        logging.debug('Notifying %d of mention in %d title by %d',
                      account_key.id(), content.key.id(), content.creator.id())
        hub = notifs.Hub(account_key)
        futures.append(hub.emit_async(notifs.ON_CONTENT_MENTION,
            creator=creator,
            content=content))
    yield tuple(futures)


@ndb.tasklet
def _notify_fb_friends_async(account, access_token):
    try:
        url = ('https://graph.facebook.com/v2.10/me?access_token=%s'
               '&fields=id,age_range,name,picture.type(large){url},friends{id}')
        result = yield ndb.get_context().urlfetch(
            method=urlfetch.GET,
            url=url % (access_token,))
    except:
        logging.exception('Failed to call Facebook API')
        raise ndb.Return(0)
    if result.status_code != 200:
        logging.error('Unexpected HTTP status code %d: %r',
                      result.status_code, result.content)
        raise ndb.Return(0)
    try:
        data = json.loads(result.content)
        fb_id = data['id']
        fb_name = data['name']
        if 'picture' in data:
            fb_image_url = data['picture']['data']['url']
        else:
            fb_image_url = None
        if 'friends' in data:
            identity_keys = [ndb.Key('Identity', 'facebook:' + d['id'])
                             for d in data['friends']['data']]
            total_friends = data['friends']['summary']['total_count']
        else:
            identity_keys = []
            total_friends = None
    except:
        logging.exception('Invalid JSON data')
        raise ndb.Return(0)
    if total_friends is not None:
        logging.debug('%s (%s) notifying %d out of %d Facebook friends',
            fb_name, fb_id, len(identity_keys), total_friends)
    else:
        logging.warning('Failed to load Facebook friends for %s (%s)',
            fb_name, fb_id)
    event_future = models.AccountEvent.create_async(account.key, 'Facebook Login',
        event_class='info',
        properties={'id': fb_id,
                    'age_range': data.get('age_range'),
                    'friends': total_friends,
                    'friends_in_app': len(identity_keys)})
    if not identity_keys:
        yield event_future
        raise ndb.Return(0)
    identities = yield ndb.get_multi_async(identity_keys)
    account_keys = set(i.account for i in identities if i and i.account)
    futures = [event_future]
    for account_key in account_keys:
        # Note: `account` is the new user, `account_key` is for one of their friends.
        future = notifs.Hub(account_key).emit_async(
            notifs.ON_FRIEND_JOINED,
            friend=account,
            friend_name=fb_name,
            friend_image_url=fb_image_url or account.image_url)
        futures.append(future)
    yield tuple(futures)
    raise ndb.Return(len(futures))


def _notify_follower_chat_async(account_key, owner, channel_id, text):
    hub = notifs.Hub(account_key)
    return hub.emit_async(notifs.ON_CHAT_OWNER_JOIN,
        channel_id=channel_id, owner=owner, text=text)


def _notify_follower_content_async(account_key, creator, content):
    hub = notifs.Hub(account_key)
    return hub.emit_async(notifs.ON_CONTENT_CREATED,
        creator=creator, content=content)


@ndb.tasklet
def _notify_requester_async(cr, creator_future, content_future):
    creator, content = yield creator_future, content_future
    if not content.is_public:
        logging.debug('Content %d no longer public', content.key.id())
        return
    logging.debug('Notifying %d that their request was fulfilled by %s',
        cr.requested_by.id(), creator.username)
    hub = notifs.Hub(cr.requested_by)
    result = yield hub.emit_async(notifs.ON_CONTENT_REQUEST_FULFILLED,
                                  creator=creator, content=content)
    # TODO: Decide if the request should be kept around for any reason.
    yield cr.key.delete_async()
    raise ndb.Return(result)


@ndb.tasklet
def _notify_team_join(auth, account):
    if auth.key.parent() == account.key:
        # Don't notify the account that just joined the team.
        return
    receiver = yield auth.key.parent().get_async()
    logging.debug('Notifying %d (%s) that %d (%s) joined %s',
                  receiver.key.id(), receiver.display_name,
                  account.key.id(), account.display_name,
                  auth.key.id())
    hub = notifs.Hub(receiver)
    yield hub.emit_async(notifs.ON_SERVICE_TEAM_JOIN,
                         account=account,
                         service_id=auth.service.id(),
                         team_id=auth.service_team.id())


def _put_content_in_search_index(creator, content):
    tags = []
    for tag in content.visible_tags:
        if ' ' in tag or tag in ('featured', 'original', 'reaction', 'reacttothis'):
            continue
        tags.append(tag)
    fields = [
        search.TextField(name='title', value=content.title),
        search.TextField(name='url', value=content.original_url),
        search.TextField(name='tags', value=','.join(tags)),
    ]
    if creator:
        fields += [
            search.AtomField(name='creator_id', value=str(creator.key.id())),
            search.AtomField(name='creator_image_url', value=creator.image_url or ''),
            search.AtomField(name='creator_username', value=creator.username or ''),
            search.AtomField(name='creator_verified', value='Y' if creator.verified else 'N'),
        ]
    thumb_url = content.thumb_url
    if thumb_url and len(thumb_url) > 400:
        thumb_url = thumb_url[:400]
    fields += [
        search.AtomField(name='thumb_url', value=thumb_url),
        search.NumberField(name='duration', value=content.duration / 1000),
        search.NumberField(name='related_count', value=content.related_count),
        search.DateField(name='created', value=content.created),
    ]
    document = search.Document(doc_id=str(content.key.id()), fields=fields, rank=content.search_rank)
    search.Index('original2').put(document)


@ndb.tasklet
def _recount_content_reactions_async(content_key):
    q = models.Content.query()
    q = q.filter(models.Content.related_to == content_key)
    q = q.filter(models.Content.tags == 'reaction')
    new_count = yield q.count_async(keys_only=True, limit=200)
    if new_count < 200:
        logging.debug('Setting reaction count for content %d to new value %d',
            content_key.id(), new_count)
        yield _set_content_reaction_count_async(content_key, new_count)


@ndb.tasklet
def _recount_account_reactions_async(account_key):
    q = models.Content.query()
    q = q.filter(models.Content.creator == account_key)
    q = q.filter(models.Content.tags == 'reaction')
    new_count = yield q.count_async(keys_only=True, limit=500)
    if new_count < 500:
        logging.debug('Setting reaction count for account %d to new value %d',
            account_key.id(), new_count)
        yield _set_account_reaction_count_async(account_key, new_count)


@ndb.tasklet
def _schedule_requests_async(account):
    cache_key = 'onboarding_request_content_ids'
    futures = []
    # Get or create a request pool to sample requests from.
    context = ndb.get_context()
    pool = yield context.memcache_get(cache_key)
    if not pool:
        q = models.Content.query()
        q = q.filter(models.Content.tags == 'original')
        q = q.filter(models.Content.tags == 'is suggestion')
        q = q.order(-models.Content.sort_index)
        content_keys = yield q.fetch_async(50, keys_only=True)
        pool = [k.id() for k in content_keys]
        futures.append(context.memcache_set(cache_key, pool, time=43200))
    if len(pool) < config.ONBOARDING_REQUESTS_COUNT:
        logging.error('Failed to get a content pool of %d or more ids', config.ONBOARDING_REQUESTS_COUNT)
        return
    # First, pick any hard coded content ids from config.
    cids = list(config.ONBOARDING_REQUESTS_INJECT_IDS)
    random.shuffle(cids)
    # Then pick random requests from the request pool.
    cids += [cid for cid in random.sample(pool, config.ONBOARDING_REQUESTS_COUNT) if cid not in cids]
    # Limit number of requests.
    cids = cids[:config.ONBOARDING_REQUESTS_COUNT]
    logging.debug('Picked %r as onboarding requests', cids)
    # Send all requests with a delay (first one immediately; rest twice per day).
    reactioncam = ndb.Key('Account', config.REACTION_CAM_ID)
    for i, cid in enumerate(cids):
        task = taskqueue.Task(
            url='/_ah/jobs/content_request_send',
            countdown=config.ONBOARDING_REQUESTS_CADENCE(i),
            params={'requester_id': reactioncam.id(),
                    'account_id': account.key.id(),
                    'content_id': cid})
        futures.append(_add_task_async(task, queue_name=config.INTERNAL_QUEUE))
    yield tuple(futures)


@ndb.tasklet
def _send_content_requests_async(requester, accounts, content_id, comment=None):
    r, a_keys, c = yield (
        models.Account.resolve_async(requester),
        models.Account.resolve_keys_async(accounts),
        ndb.Key('Content', content_id).get_async())
    if not r or not a_keys or not c:
        logging.error('Failed to get account(s) or content %r', content_id)
        raise ndb.Return(None)
    futures = []
    for k in a_keys:
        h = notifs.Hub(k)
        f = h.emit_async(notifs.ON_CONTENT_REQUEST, comment=comment, content=c, requester=r)
        futures.append(f)
    cr_list = [models.ContentRequest(content=c.key, requested_by=r.key, parent=k) for k in a_keys]
    futures.append(ndb.put_multi_async(cr_list))
    yield tuple(futures)
    raise ndb.Return(cr_list)


@ndb.tasklet
def _send_review_to_slack_async(content, creator):
    p = '%d:%d:' % (creator.key.id(), content.key.id())
    vote = slack_api.attachment(
        '%s needs review' % (slack_api.admin(creator),),
        author_icon=creator.image_url,
        author_name='@%s' % (creator.username,),
        author_link='https://api.reaction.cam/admin/accounts/%d/' % (creator.key.id(),),
        title=content.title or 'Untitled Video',
        title_link='https://api.reaction.cam/admin/content/%d/' % (content.key.id(),),
        image_url=content.thumb_url,
        callback_id='review_content',
        actions=[
            {'name': 'quality', 'value': p+'0', 'text': u'1️⃣', 'type': 'button'},
            {'name': 'quality', 'value': p+'1', 'text': u'2️⃣', 'type': 'button'},
            {'name': 'quality', 'value': p+'2', 'text': u'3️⃣', 'type': 'button'},
            {'name': 'quality', 'value': p+'3', 'text': u'4️⃣', 'type': 'button'},
            {'name': 'quality', 'value': p+'4', 'text': u'🤩', 'type': 'button'},
            {'name': 'quality', 'value': p+'hide', 'text': 'Skip', 'type': 'button'},
        ],
    )
    yield slack_api.message_async(attachments=[vote], hook_id='review')


@ndb.tasklet
def _publish_recent_content_async(account_key, quality):
    q = models.Content.query()
    q = q.filter(models.Content.creator == account_key)
    q = q.filter(models.Content.tags == 'reaction')
    q = q.order(-models.Content.created)
    content_list = yield q.fetch_async(5)
    context = ndb.get_context()
    futures = []
    for content in content_list:
        tags_before = set(content.tags)
        content.add_tag('published', allow_restricted=True)
        if set(content.tags) != tags_before:
            futures.append(content.put_async())
        url = 'https://bn.reaction.cam/v1/rate?id=%d&rating=%d' % (content.key.id(), quality + 1)
        futures.append(context.urlfetch(method='POST', url=url))
    for f in futures:
        try:
            yield f
        except:
            logging.exception('Failed to publish content.')


@ndb.transactional_tasklet
def _set_account_reaction_count_async(account_key, new_count):
    account = yield account_key.get_async()
    account.content_count = new_count
    yield account.put_async()


@ndb.transactional_tasklet
def _set_account_quality_async(account_key, quality):
    account = yield account_key.get_async()
    if not account.quality_has_been_set or quality != account.quality:
        account.quality = quality
        yield account.put_async()
    raise ndb.Return(account)


@ndb.transactional_tasklet
def _set_content_reaction_count_async(content_key, new_count):
    content = yield content_key.get_async()
    if new_count == 0:
        content.remove_tag('is reacted', allow_restricted=True)
    content.related_count = new_count
    yield content.put_async()


@ndb.transactional_tasklet
def _set_account_youtube_channel_async(account_key, channel):
    account = yield account_key.get_async()
    if (channel['id'] == account.youtube_channel_id and
        channel['thumb_url'] == account.youtube_channel_thumb_url and
        channel['title'] == account.youtube_channel_title and
        channel['views'] == account.youtube_channel_views and
        channel['subs'] == account.youtube_subs):
        return
    account.youtube_channel_id = channel['id']
    account.youtube_channel_thumb_url = channel['thumb_url']
    account.youtube_channel_title = channel['title']
    account.youtube_channel_views = channel['views']
    account.youtube_subs = channel['subs']
    account.youtube_subs_updated = datetime.utcnow()
    yield account.put_async()
    logging.debug('Updated YouTube channel for %d: %r', account_key.id(), channel)


@ndb.transactional_tasklet
def _set_content_thumbnail_async(content_key, thumb_url):
    content = yield content_key.get_async()
    if content.thumb_url:
        logging.debug('Aborting thumbnail update (changed elsewhere)')
        raise ndb.Rollback()
    content.thumb_url = thumb_url
    yield content.put_async()
    logging.debug('Set thumbnail for %d: %s', content.key.id(), content.thumb_url)


@ndb.transactional_tasklet
def _update_streak_async(account_key, first=False):
    account = yield account_key.get_async()
    account.content_count += 1
    if first:
        account.total_content_found += 1
    if account.location_info:
        tz = pytz.timezone(account.location_info.timezone)
    else:
        tz = pytz.timezone('America/New_York')
    user_time = pytz.utc.localize(datetime.utcnow()).astimezone(tz)
    user_date = user_time.date()
    if account.streak_time:
        last_date = pytz.utc.localize(account.streak_time).astimezone(tz).date()
        before = account.streak_count
        if user_date != last_date:
            if (user_date - last_date).days == 1:
                account.streak_count += 1
            else:
                account.streak_count = 1
    else:
        before = 0
        account.streak_count = 1
    account.streak_max = max(account.streak_max, account.streak_count)
    account.streak_time = user_time.astimezone(pytz.utc).replace(tzinfo=None)
    yield account.put_async()
    raise ndb.Return((account, before, account.streak_count))


@ndb.transactional_tasklet(xg=True)
def _update_youtube_reaction_views_async(deltas):
    # NOTE: This can be either Account or Content!
    keys = deltas.keys()
    entities = yield ndb.get_multi_async(keys)
    for key, entity in zip(keys, entities):
        if not entity:
            logging.error('Missing entity: %r', key)
            continue
        views = entity.youtube_reaction_views or 0
        views += deltas[entity.key]
        entity.youtube_reaction_views = views
        entity.youtube_reaction_views_updated = datetime.utcnow()
    yield ndb.put_multi_async(e for e in entities if e)


@ndb.tasklet
def _update_youtube_views_async(content_ids):
    content_list = yield ndb.get_multi_async(ndb.Key('Content', cid) for cid in content_ids)
    if not all(content_list):
        raise ValueError('Got one or more invalid content ids')
    # Filter out content that has no YouTube id or was updated very recently.
    def should_update(content):
        if not content.youtube_id:
            return False
        if not content.youtube_views_updated:
            return True
        if datetime.utcnow() - content.youtube_views_updated < timedelta(minutes=5):
            return False
        return True
    content_list = filter(should_update, content_list)
    if not content_list:
        # Nothing to do.
        return
    get_stats_async = lambda vid: youtube.get_video_async(vid, snippet=False, statistics=True)
    youtube_futures = map(get_stats_async, [c.youtube_id for c in content_list])
    creator_deltas = defaultdict(int)
    original_deltas = defaultdict(int)
    total_views = 0
    futures = []
    to_put = []
    for content, f in zip(content_list, youtube_futures):
        info = yield f
        if not info:
            logging.warning('Content %d YouTube id %s does not exist',
                            content.key.id(), content.youtube_id)
            if not content.youtube_broken:
                event_future = models.AccountEvent.create_async(
                    content.creator, 'YouTube Broken Video',
                    event_class='warning',
                    properties={'ContentId': str(content.key.id()),
                                'VideoId': content.youtube_id})
                futures.append(event_future)
            content.youtube_broken = True
            to_put.append(content)
            continue
        content.youtube_broken = False
        views = int(info['statistics']['viewCount'])
        if views == content.youtube_views:
            continue
        delta = content.set_youtube_views(views)
        if delta > 0:
            creator_deltas[content.creator] += delta
            if content.related_to:
                original_deltas[content.related_to] += delta
            total_views += delta
        to_put.append(content)
    if not to_put:
        # Nothing was updated.
        return
    for content in content_list:
        if content.key not in original_deltas:
            continue
        # This content has already been loaded so don't get it again.
        delta = original_deltas[content.key]
        del original_deltas[content.key]
        views = content.youtube_reaction_views or 0
        views += delta
        content.youtube_reaction_views = views
        content.youtube_reaction_views_updated = datetime.utcnow()
        # Ensure that the content is being put.
        if content not in to_put:
            to_put.append(content)
    futures.extend(ndb.put_multi_async(to_put))
    futures.append(_update_youtube_reaction_views_async(creator_deltas))
    futures.append(_update_youtube_reaction_views_async(original_deltas))
    # Make sure everything is complete.
    for f in futures:
        try:
            yield f
        except Exception:
            logging.exception('Error in Future')
    # Log debugging info.
    logging.debug('Updated %d+%d content(s) and %d account(s) with %d total view(s)',
                  len(to_put), len(original_deltas), len(creator_deltas), total_views)


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
