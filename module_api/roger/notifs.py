# -*- coding: utf-8 -*-

from collections import defaultdict
from datetime import date
import itertools
import json
import logging

from google.appengine.api import taskqueue
from google.appengine.ext import ndb

from roger import config, external, models, push_service
from roger_common import convert, events, identifiers


CUSTOM                        = 'custom'
ON_ACCOUNT_CHANGE             = 'account-change'
ON_ACCOUNT_FOLLOW             = 'account-follow'
ON_ACCOUNT_STATUS_CHANGE      = 'status-change'
ON_CHAT_MENTION               = 'chat-mention'
ON_CHAT_MESSAGE               = 'chat-message'
ON_CHAT_JOIN                  = 'chat-join'
ON_CHAT_OWNER_JOIN            = 'chat-owner-join'
ON_CONTENT_COMMENT            = 'content-comment'
ON_CONTENT_CREATED            = 'content-created'
ON_CONTENT_FEATURED           = 'content-featured'
ON_CONTENT_MENTION            = 'content-mention'
ON_CONTENT_REFERENCED         = 'content-referenced'
ON_CONTENT_REQUEST            = 'content-request'
ON_CONTENT_REQUEST_FULFILLED  = 'content-request-fulfilled'
ON_CONTENT_VIEW               = 'content-view'
ON_CONTENT_VOTE               = 'content-vote'
ON_FRIEND_JOINED              = 'friend-joined'
ON_PUBLIC_REQUEST_UPDATE      = 'public-request-update'
ON_SERVICE_TEAM_JOIN          = 'service-team-join'
ON_STREAK                     = 'streak'
ON_STREAM_ATTACHMENT          = 'stream-attachment'
ON_STREAM_BUZZ                = 'stream-buzz'
ON_STREAM_CHANGE              = 'stream-change'
ON_STREAM_CHUNK               = 'stream-chunk'
ON_STREAM_CHUNK_EXTERNAL_PLAY = 'stream-chunk-external-play'
ON_STREAM_CHUNK_FIRST_PLAY    = 'stream-chunk-first-play'
ON_STREAM_CHUNK_REACTION      = 'stream-chunk-reaction'
ON_STREAM_CHUNK_TEXT          = 'stream-chunk-text'
ON_STREAM_HIDDEN              = 'stream-hidden'
ON_STREAM_IMAGE               = 'stream-image'
ON_STREAM_JOIN                = 'stream-join'
ON_STREAM_LEAVE               = 'stream-leave'
ON_STREAM_PLAY                = 'stream-listen'
ON_STREAM_NEW                 = 'stream-new'
ON_STREAM_PARTICIPANT_CHANGE  = 'stream-participant-change'
ON_STREAM_PARTICIPANTS        = 'stream-participants'
ON_STREAM_SHAREABLE           = 'stream-shareable'
ON_STREAM_SHOWN               = 'stream-shown'
ON_STREAM_STATUS              = 'stream-status'
ON_STREAM_TITLE               = 'stream-title'
ON_THREAD_MESSAGE             = 'thread-message'


STREAM_CHUNK_EVENTS = {
    ON_STREAM_CHUNK,
    ON_STREAM_CHUNK_EXTERNAL_PLAY,
    ON_STREAM_CHUNK_FIRST_PLAY,
    ON_STREAM_CHUNK_REACTION,
    ON_STREAM_CHUNK_TEXT,
}


# Stream change events that include the full stream data.
STREAM_DATA_EVENTS = {
    ON_STREAM_BUZZ,
    ON_STREAM_CHANGE,
    ON_STREAM_IMAGE,
    ON_STREAM_PLAY,
    ON_STREAM_SHAREABLE,
    ON_STREAM_TITLE,
}


JSON_TEMPLATE = '{"account_id": %d, "app": %s, "device_token": %s, "environment": %s, "data": %s}'


_handlers = defaultdict(list)
def add_handler(event_type, handler):
    _handlers[event_type].append(handler)


def _reactioncam_notif(me_key, device, event, badge=None):
    origin_account = None
    title, body, sound = None, None, None
    if event.event_type == CUSTOM:
        data = dict(event.data)
        alert_title = data.pop('title')
        alert_body = data.pop('text')
        alert_disabled = data.pop('alert_disabled', False)
        alert_sound = data.pop('alert_sound', True)
        if not alert_disabled:
            title = alert_title
            body = alert_body
            if alert_sound:
                sound = 'default'
        data.pop('group_key', None)
        data.pop('group_history_keys', None)
        event.data = data
    elif event.event_type == ON_ACCOUNT_FOLLOW:
        follower = event.data['follower']
        body = u'@%s subscribed to you' % (follower.username,)
        origin_account = follower
    elif event.event_type == ON_CHAT_JOIN:
        joiner = event.data['joiner']
        title = u'@%s joined your LIVECHAT 🔴' % (joiner.username,)
        body = u'Go there now to reply to them!'
        sound = 'default'
    elif event.event_type in (ON_CHAT_MENTION, ON_CHAT_MESSAGE):
        owner = event.data['owner']
        sender = event.data['sender']
        if me_key == owner.key:
            title = u'LIVECHAT 🔴'
        else:
            title = u'@%s’s LIVECHAT 🔴' % (owner.username,)
        body = u'@%s: %s' % (sender.username, event.data['text'])
        sound = 'default'
    elif event.event_type == ON_CHAT_OWNER_JOIN:
        owner = event.data['owner']
        title = u'@%s is on LIVECHAT 🔴' % (owner.username,)
        body = event.data['text']
    elif event.event_type == ON_CONTENT_COMMENT:
        commenter = event.data['commenter']
        if commenter.key != me_key:
            comment = event.data['comment']
            content = event.data['content']
            if content.creator == me_key:
                title = content.title or u'New Comment'
                body = u'@%s: %s' % (commenter.username, comment.text)
                sound = 'default'
            else:
                # Assume that if notified user didn't create content,
                # they're getting the notif because they also commented.
                body = u'@%s also commented: %s' % (commenter.username, comment.text)
            origin_account = commenter
    elif event.event_type == ON_CONTENT_CREATED:
        content = event.data['content']
        creator = event.data['creator']
        if creator.key != me_key:
            if 'repost' in content.tags:
                if content.title:
                    title = u'@%s reposted a video' % (creator.username,)
                    body = content.title[:100]
                else:
                    body = u'@%s reposted a video.' % (creator.username,)
            elif content.title:
                title = u'@%s posted a video' % (creator.username,)
                body = content.title[:100]
            else:
                body = u'@%s just posted a new video!' % (creator.username,)
            origin_account = creator
    elif event.event_type == ON_CONTENT_FEATURED:
        title = u'FEATURED 🏆'
        body = u'Your video just got featured!'
        sound = 'default'
    elif event.event_type == ON_CONTENT_MENTION:
        creator = event.data['creator']
        if creator.key != me_key:
            body = u'@%s mentioned you in their video!' % (creator.username,)
            sound = 'default'
            origin_account = creator
    elif event.event_type == ON_CONTENT_REFERENCED:
        creator = event.data['creator']
        if creator.key != me_key:
            body = u'@%s reacted to your video! 🙌' % (creator.username,)
            sound = 'default'
            origin_account = creator
    elif event.event_type == ON_CONTENT_REQUEST:
        comment = event.data.get('comment') or None
        content = event.data['content']
        requester = event.data['requester']
        if requester.key != me_key:
            if content.title:
                body = u'@%s requested your reaction to %s' % (requester.username, content.title)
            else:
                body = u'@%s requested your reaction' % (requester.username,)
            if comment:
                title = body
                body = comment
            origin_account = requester
    elif event.event_type == ON_CONTENT_REQUEST_FULFILLED:
        creator = event.data['creator']
        if creator.key != me_key:
            body = u'@%s made the reaction you requested! 🙌' % (creator.username,)
            sound = 'default'
            origin_account = creator
    elif event.event_type == ON_CONTENT_VIEW:
        pass
    elif event.event_type == ON_CONTENT_VOTE:
        content = event.data['content']
        voter = event.data['voter']
        if voter.key != me_key:
            body = u'@%s liked your video' % (voter.username,)
            origin_account = voter
    elif event.event_type == ON_FRIEND_JOINED:
        title = u'FRIEND JOINED 🙌'
        body = u'%s joined you on reaction.cam' % (event.data['friend_name'],)
        sound = 'default'
        origin_account = event.data['friend']
    elif event.event_type == ON_STREAK:
        body = u'🚨 You’re on a %d-day posting streak!' % (event.data['days'],)
        sound = 'default'
    elif event.event_type == ON_THREAD_MESSAGE:
        thread = event.data['thread']
        message = event.data['message']
        sender_key = message.account
        if sender_key != me_key:
            for thread_account in thread.accounts:
                if thread_account.account == sender_key:
                    break
            else:
                # Something went wrong...
                return []
            title = u'@%s' % (thread_account.username,)
            body = message.text
            sound = 'default'
    if body:
        event.data['aps'] = {'alert': {'body': body}}
        if title:
            event.data['aps']['alert']['title'] = title
        if badge is not None:
            event.data['aps']['badge'] = badge
        if sound:
            event.data['aps']['sound'] = sound
    else:
        event.data['aps'] = {'content-available': 1}
    if origin_account and me_key in origin_account.blocked_by:
        # Don't send any push notifications caused by a blocked user.
        return []
    payloads = [_to_json(me_key, device, event)]
    return payloads


def _to_json(account_key, device, custom):
    public_options = dict(custom._public_options) if isinstance(custom, Event) else {}
    public_options['num_chunks'] = 0
    public_options['version'] = device.api_version
    return JSON_TEMPLATE % (
            account_key.id(),
            json.dumps(device.app),
            json.dumps(device.key.id()),
            json.dumps(device.environment),
            convert.to_json(custom, **public_options))


@ndb.tasklet
def _push_batch_async(todo, options):
    if not todo:
        raise RuntimeError('Nothing to do.')
    notif_futures = []
    for _, (account_key, custom) in todo:
        notif_futures.append(_create_account_notif_async(account_key, custom))
    device_futures = []
    for _, (account_key, custom) in todo:
        device_futures.append(models.Device.query(ancestor=account_key).fetch_async())
    notif_results = yield tuple(notif_futures)
    badge_keys = []
    badge_futures = []
    for ((_, (account_key, _)), did_create_notif) in itertools.izip(todo, notif_results):
        if did_create_notif:
            badge_keys.append(account_key)
            badge_futures.append(models.AccountNotification.count_unseen_async(account_key))
    badge_results = yield tuple(badge_futures)
    badge_map = dict(itertools.izip(badge_keys, badge_results))
    device_results = yield tuple(device_futures)
    push_futures = []
    for ((_, (account_key, event)), device_list) in itertools.izip(todo, device_results):
        for device in device_list:
            if device.platform != 'ios':
                logging.debug('Unsupported platform %r', device.platform)
                continue
            if not hasattr(event, 'event_type'):
                logging.error('Unsupported event data (missing event_type)')
                continue
            if device.app == 'cam.reaction.ReactionCam':
                badge = badge_map.get(account_key)
                original_data = event.data
                event.data = dict(original_data)
                payloads = _reactioncam_notif(account_key, device, event, badge=badge)
                event.data = original_data
            else:
                logging.error('Unsupported app %r', device.app)
                continue
            for json_string in payloads:
                push_futures.append(push_service.post_async(json_string))
    yield tuple(push_futures)
    for future, _ in todo:
        future.set_result(None)


_batcher = ndb.AutoBatcher(_push_batch_async, 100)


def push_async(account_key, custom=None):
    return _batcher.add((account_key, custom), ())


@ndb.tasklet
def _add_task_async(task, queue_name):
    yield task.add_async(queue_name=queue_name)


@ndb.tasklet
def _create_account_notif_async(account_key, event):
    if not hasattr(event, 'event_type'):
        raise ndb.Return(False)
    notif = models.AccountNotification.new(account_key, event.event_type)
    decorated = _decorate_account_notif(account_key, notif, event.data)
    if not decorated:
        raise ndb.Return(False)
    if notif.group_key:
        yield models.AccountNotification.put_grouped_async(notif)
    else:
        yield notif.put_async()
    raise ndb.Return(True)


def _decorate_account_notif(account_key, notif, data):
    origin_account = None
    if notif.type == CUSTOM:
        props = dict(data)
        if props.pop('notif_disabled', False):
            return False
        props.pop('alert_disabled', None)
        props.pop('alert_open_url', None)
        props.pop('alert_sound', None)
        notif.group_key = props.pop('group_key', None)
        notif.group_history_keys = props.pop('group_history_keys', [])
        notif.properties = props
        logging.debug('Custom notification: %r', data)
    elif notif.type == ON_ACCOUNT_FOLLOW:
        follower = data['follower']
        notif.properties['follower_id'] = follower.key.id()
        notif.properties['follower_image_url'] = follower.image_url
        notif.properties['follower_username'] = follower.username
        notif.properties['follower_verified'] = follower.verified
        notif.group_key = str(date.today())
        notif.group_history_keys = ['follower_id', 'follower_image_url', 'follower_username', 'follower_verified']
        origin_account = follower
    elif notif.type == ON_CHAT_JOIN:
        joiner = data['joiner']
        owner = data['owner']
        notif.properties['channel_id'] = data['channel_id']
        notif.properties['joiner_id'] = joiner.key.id()
        notif.properties['joiner_image_url'] = joiner.image_url
        notif.properties['joiner_username'] = joiner.username
        notif.properties['joiner_verified'] = joiner.verified
        notif.properties['owner_id'] = owner.key.id()
        notif.properties['owner_image_url'] = owner.image_url
        notif.properties['owner_username'] = owner.username
        notif.properties['owner_verified'] = owner.verified
        notif.properties['text'] = data['text']
        notif.group_key = data['channel_id']
        origin_account = joiner
    elif notif.type in (ON_CHAT_MENTION, ON_CHAT_MESSAGE):
        owner = data['owner']
        sender = data['sender']
        notif.properties['channel_id'] = data['channel_id']
        notif.properties['owner_id'] = owner.key.id()
        notif.properties['owner_image_url'] = owner.image_url
        notif.properties['owner_username'] = owner.username
        notif.properties['owner_verified'] = owner.verified
        notif.properties['sender_id'] = sender.key.id()
        notif.properties['sender_image_url'] = sender.image_url
        notif.properties['sender_username'] = sender.username
        notif.properties['sender_verified'] = sender.verified
        notif.properties['text'] = data['text']
        notif.group_key = data['channel_id']
        origin_account = sender
    elif notif.type == ON_CHAT_OWNER_JOIN:
        owner = data['owner']
        notif.properties['channel_id'] = data['channel_id']
        notif.properties['owner_id'] = owner.key.id()
        notif.properties['owner_image_url'] = owner.image_url
        notif.properties['owner_username'] = owner.username
        notif.properties['owner_verified'] = owner.verified
        notif.properties['text'] = data['text']
        notif.group_key = data['channel_id']
        origin_account = owner
    elif notif.type == ON_CONTENT_COMMENT:
        commenter = data['commenter']
        if commenter.key == account_key:
            return False
        comment = data['comment']
        content = data['content']
        notif.properties['comment_id'] = comment.key.id()
        notif.properties['comment_offset'] = comment.offset
        notif.properties['comment_text'] = comment.text
        notif.properties['content_id'] = content.key.id()
        notif.properties['content_thumb_url'] = content.thumb_url
        notif.properties['content_title'] = content.title
        notif.properties['commenter_id'] = commenter.key.id()
        notif.properties['commenter_image_url'] = commenter.image_url
        notif.properties['commenter_username'] = commenter.username
        notif.properties['commenter_verified'] = commenter.verified
        origin_account = commenter
    elif notif.type in (ON_CONTENT_CREATED, ON_CONTENT_MENTION):
        creator = data['creator']
        if creator.key == account_key:
            return False
        content = data['content']
        notif.properties['content_id'] = content.key.id()
        notif.properties['content_thumb_url'] = content.thumb_url
        notif.properties['content_title'] = content.title
        notif.properties['content_url'] = content.original_url or content.video_url
        notif.properties['creator_id'] = creator.key.id()
        notif.properties['creator_image_url'] = creator.image_url
        notif.properties['creator_username'] = creator.username
        notif.properties['creator_verified'] = creator.verified
        origin_account = creator
    elif notif.type == ON_CONTENT_FEATURED:
        content = data['content']
        notif.properties['content_id'] = content.key.id()
        notif.properties['content_thumb_url'] = content.thumb_url
        notif.properties['content_title'] = content.title
    elif notif.type == ON_CONTENT_REFERENCED:
        creator = data['creator']
        if creator.key == account_key:
            return False
        content = data['content']
        notif.properties['content_id'] = content.key.id()
        notif.properties['content_thumb_url'] = content.thumb_url
        notif.properties['content_title'] = content.title
        notif.properties['creator_id'] = creator.key.id()
        notif.properties['creator_image_url'] = creator.image_url
        notif.properties['creator_username'] = creator.username
        notif.properties['creator_verified'] = creator.verified
        notif.properties['original_id'] = content.related_to.id()
        origin_account = creator
    elif notif.type == ON_CONTENT_REQUEST:
        content = data['content']
        requester = data['requester']
        notif.properties['comment'] = data.get('comment')
        notif.properties['content_id'] = content.key.id()
        notif.properties['content_thumb_url'] = content.thumb_url
        notif.properties['content_title'] = content.title
        notif.properties['content_url'] = content.original_url or content.video_url
        notif.properties['requester_id'] = requester.key.id()
        notif.properties['requester_image_url'] = requester.image_url
        notif.properties['requester_username'] = requester.username
        notif.properties['requester_verified'] = requester.verified
        origin_account = requester
    elif notif.type == ON_CONTENT_REQUEST_FULFILLED:
        creator = data['creator']
        if creator.key == account_key:
            return False
        content = data['content']
        notif.properties['content_id'] = content.key.id()
        notif.properties['content_thumb_url'] = content.thumb_url
        notif.properties['content_title'] = content.title
        notif.properties['creator_id'] = creator.key.id()
        notif.properties['creator_image_url'] = creator.image_url
        notif.properties['creator_username'] = creator.username
        notif.properties['creator_verified'] = creator.verified
        notif.properties['original_id'] = content.related_to.id()
        origin_account = creator
    elif notif.type == ON_CONTENT_VOTE:
        voter = data['voter']
        if voter.key == account_key:
            return False
        content = data['content']
        notif.properties['content_id'] = content.key.id()
        notif.properties['content_thumb_url'] = content.thumb_url
        notif.properties['content_title'] = content.title
        notif.properties['voter_id'] = voter.key.id()
        notif.properties['voter_image_url'] = voter.image_url
        notif.properties['voter_username'] = voter.username
        notif.properties['voter_verified'] = voter.verified
        notif.group_key = content.key.id()
        notif.group_history_keys = ['voter_id', 'voter_image_url', 'voter_username', 'voter_verified']
        origin_account = voter
    elif notif.type == ON_FRIEND_JOINED:
        friend = data['friend']
        notif.properties['friend_id'] = friend.key.id()
        notif.properties['friend_name'] = data['friend_name']
        notif.properties['friend_image_url'] = data['friend_image_url']
        origin_account = friend
    elif notif.type == ON_STREAK:
        notif.properties['days'] = data['days']
        notif.group_key = 'streak'
    else:
        return False
    if origin_account and account_key in origin_account.blocked_by:
        # Don't show activity caused by blocked accounts.
        return False
    return True


class Event(object):
    __slots__ = ['data', 'event_type', 'event_account_key', '_account', '_json_cache', '_public_options']

    def __getattr__(self, name):
        return self.data[name]

    def __init__(self, event_type, event_account, public_options={}, **kwargs):
        if isinstance(event_account, models.Account):
            self._account = event_account
            self.event_account_key = event_account.key
        else:
            self._account = None
            self.event_account_key = models.Account.resolve_key(event_account)
            if not self.event_account_key:
                raise ValueError('Expected a valid Account key or instance')
        self.event_type = event_type
        self.data = kwargs
        self._json_cache = {}
        self._public_options = public_options

    @property
    def event_account(self):
        if not self._account:
            # TODO: Propagate this to Hub.
            self._account = self.event_account_key.get()
            logging.debug('notifs.Event loaded account %d', self.event_account_key.id())
        return self._account

    def get_json(self, version):
        if version not in self._json_cache:
            self._json_cache[version] = convert.to_json(self, version=version,
                                                        **self._public_options)
        return self._json_cache[version]

    def public(self, **kwargs):
        return EventDataWrapper(self.event_type, **self.data)


class EventDataWrapper(object):
    __slots__ = ['data', 'event_type']

    def __init__(self, event_type, **kwargs):
        self.event_type = event_type
        # Don't include certain options with the data passed down to clients.
        kwargs.pop('mute_notification', None)
        self.data = kwargs

    def public(self, version=None, **kwargs):
        return dict(type=self.event_type, api_version=version, **self.data)


class StreamEvent(object):
    __slots__ = ['data', 'event_type', 'stream']

    def __init__(self, *args, **kwargs):
        if len(args) == 1 and len(kwargs) == 0 and isinstance(args[0], Event):
            event = args[0]
        else:
            event = Event(*args, **kwargs)
        data = event.data.copy()
        self.event_type = event.event_type
        self.stream = data.pop('stream')
        self.data = data


class StreamChangeEvent(StreamEvent):
    def public(self, num_chunks=None, version=None, **kwargs):
        # Custom logic to strip out lists.
        stream_dict = self.stream.public(num_chunks=0, version=version, **kwargs)
        if version >= 8:
            del stream_dict['chunks']
            del stream_dict['others']
        if version >= 16:
            del stream_dict['attachments']
        return EventDataWrapper(self.event_type, stream=stream_dict, **self.data)


class StreamChunkEvent(StreamEvent):
    def public(self, num_chunks=None, version=None, **kwargs):
        # Custom logic to strip out the stream.
        return EventDataWrapper(self.event_type, stream_id=self.stream.key.id(),
                                **self.data)


class Hub(object):
    def __init__(self, account):
        if isinstance(account, models.Account):
            self._account = account
            self.account_key = account.key
        else:
            self._account = None
            self.account_key = models.Account.resolve_key(account)
            if not self.account_key:
                raise ValueError('Expected a valid Account key or instance')

    @property
    def account(self):
        if not self._account:
            self._account = self.account_key.get()
            logging.debug('notifs.Hub loaded account %d', self.account_key.id())
        return self._account

    @property
    def account_or_key(self):
        return self._account or self.account_key

    def emit(self, event_type, **kwargs):
        return self.emit_async(event_type, **kwargs).get_result()

    @ndb.tasklet
    def emit_async(self, event_type, **kwargs):
        # Notify the account via each notifier.
        e = Event(event_type, self.account_or_key, **kwargs)
        for handler in _handlers[event_type]:
            handler(e)
        if config.DEVELOPMENT:
            logging.info('%s: %s', e.event_type, e.get_json(44))
        try:
            if e.event_type in STREAM_CHUNK_EVENTS:
                custom = StreamChunkEvent(e)
            elif e.event_type in STREAM_DATA_EVENTS:
                custom = StreamChangeEvent(e)
            else:
                custom = e
            yield push_async(e.event_account_key, custom=custom)
        except:
            logging.exception('Failed to notify via push')
