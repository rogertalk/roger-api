# -*- coding: utf-8 -*-

from datetime import datetime, timedelta
from itertools import dropwhile
import json
import logging
import os.path
import urllib

from google.appengine.api import taskqueue, urlfetch
from google.appengine.datastore.datastore_query import Cursor
from google.appengine.ext import deferred, ndb

from roger import config, files, models, notifs
from roger_common import convert, errors, events, identifiers, reporting


# TODO: Remove.
SERVICE_ACCOUNTS = {
    ndb.Key('Account', 61840001):  ndb.Key('Service', 'alexa'),
    ndb.Key('Account', 23360009):  ndb.Key('Service', 'chewbacca'),
    ndb.Key('Account', 682920001): ndb.Key('Service', 'ifttt'),
    ndb.Key('Account', 343460002): ndb.Key('Service', 'messenger'),
    ndb.Key('Account', 355150003): ndb.Key('Service', 'share'),
}


SHARE_STREAM_TITLE = '%ShareExternally%'


def get_by_invite_token(invite_token, *args, **kwargs):
    stream = models.Stream.query(models.Stream.invite_token == invite_token).get()
    if not stream:
        raise errors.ResourceNotFound('Invalid invite token')
    return Stream(stream)


def get_featured():
    q = models.Stream.query()
    q = q.filter(models.Stream.featured > 0)
    q = q.order(models.Stream.featured)
    return [Stream(stream) for stream in q]


def get_handler(account):
    return StreamsHandler(account)


def get(account, *args, **kwargs):
    return StreamsHandler(account).get(*args, **kwargs)
def get_by_id(account, *args, **kwargs):
    return StreamsHandler(account).get_by_id(*args, **kwargs)
def get_or_create(account, *args, **kwargs):
    return StreamsHandler(account).get_or_create(*args, **kwargs)
def get_recent(account, *args, **kwargs):
    return StreamsHandler(account).get_recent(*args, **kwargs)
def get_unplayed_count(account, *args, **kwargs):
    return StreamsHandler(account).get_unplayed_count(*args, **kwargs)
def join_service_content(account, *args, **kwargs):
    return StreamsHandler(account).join_service_content(*args, **kwargs)
def send(account, *args, **kwargs):
    return StreamsHandler(account).send(*args, **kwargs)


def validate(func):
    """Decorator that validates the current account may perform the action."""
    def wrap(self, *args, **kwargs):
        if not self.participant:
            raise errors.ForbiddenAction('Account is not a participant in stream')
        if len(self._stream.participants) == 2:
            for p in self._stream.participants:
                if p.account in self.account.blocked_by:
                    logging.info('Blocked Stream.%s from %s to %s', func.__name__,
                                 self.account.key, p.account)
                    return
        return func(self, *args, **kwargs)
    return wrap


class ParticipantAccount(object):
    __slots__ = ['account', 'participant']

    def __getattr__(self, name):
        return getattr(self.account, name)

    def __init__(self, participant, account):
        self.account = account
        self.participant = participant

    def public(self, version=None, **kwargs):
        if version < 20:
            return self.account
        data = dict(
            self.account.public(version=version, **kwargs),
            owner_id=self.participant.owner.id() if self.participant.owner else None)
        if version >= 23:
            data['played_until'] = self.participant.played_until
            data['played_until_changed'] = self.participant.played_until_changed
        return data


class Stream(object):
    # Avoid accidentally setting unsupported attributes.
    __slots__ = ['_account_map', '_chunks', '_stream']

    def __getattr__(self, name):
        # By default, proxy to the underlying Stream entity.
        return getattr(self._stream, name)

    def __init__(self, stream, account_map=None, chunks=None):
        if not isinstance(stream, models.Stream):
            raise TypeError('Expected a stream')
        self._stream = stream
        if not account_map:
            account_keys = [p.account for p in stream.participants]
            account_map = dict(zip(account_keys, ndb.get_multi(account_keys)))
        self._account_map = account_map
        self._chunks = chunks

    @property
    def chunks(self):
        if not self._chunks:
            # Hide chunks that are too old.
            chunks = dropwhile(lambda c: c.is_expired, self._stream.chunks)
            self._chunks = list(chunks)
        return self._chunks

    def for_participant(self, participant, disable_autojoin=True):
        return MutableStream(participant, self._stream,
                             account_map=self._account_map, chunks=self._chunks,
                             disable_autojoin=disable_autojoin)

    def get_accounts(self, exclude_account=None):
        if exclude_account:
            exclude_account = models.Account.resolve_key(exclude_account)
        self._lookup_accounts(self.participants)
        return [ParticipantAccount(p, self._account_map[p.account])
                for p in self.participants
                if not exclude_account or p.account != exclude_account]

    def get_chunk_url(self, chunk):
        if not chunk.token:
            return None
        sender = self.lookup_account(chunk.sender)
        identifier = sender.username or str(sender.key.id())
        return '{}/{}/{}'.format(config.WEB_HOST, identifier, chunk.token)

    def get_chunk_uuid(self, chunk):
        if isinstance(chunk, models.ChunkInStream):
            chunk_id = chunk.chunk_id
        else:
            chunk_id = chunk.key.id()
        return '{}_{}'.format(self._stream.key.id(), chunk_id)

    def get_participant(self, participant):
        key = models.Account.resolve_key(participant)
        # Search participants for the account key.
        for p in self._stream.participants:
            if p.account == key:
                return p

    @property
    def has_chunks(self):
        return len(self.chunks) > 0

    def has_participant(self, participant):
        return bool(self.get_participant(participant))

    def join(self, account, reason='unknown', **kwargs):
        # TODO: Do something with reason parameter.
        # TODO: Decide on 1:1 restrictions.
        account = models.Account.resolve(account)
        self._account_map[account.key] = account
        # Create a stream object specifically for the account that joined.
        stream = MutableStream(account, self._stream, account_map=self._account_map,
                               chunks=self._chunks)
        if not stream.participant:
            stream._set_participants(add=[account], **kwargs)
        return stream

    def load_all_chunks(self):
        threshold = datetime.utcnow() - config.CHUNK_MAX_AGE
        q = models.Chunk.query(models.Chunk.start >= threshold, ancestor=self._stream.key)
        q = q.order(models.Chunk.start)
        self._chunks = q.fetch()

    def lookup_account(self, value):
        return self._lookup_accounts([value])[0]

    @property
    def presentation_title(self):
        if self.title:
            return self.title
        return ', '.join(a.display_name for a in self.get_accounts())

    def public(self, num_chunks=5, version=None, **kwargs):
        return {
            'chunks': self.chunks[-num_chunks:],
            'created': self.created,
            'id': self.key.id(),
            'image_url': self.image_url,
            'invite_token': self.invite_token,
            'participants': self.get_accounts(),
            'title': self.title,
            'total_duration': self.total_duration,
        }

    @property
    def service_id(self):
        return self.service.id() if self.service else None

    def _lookup_accounts(self, values):
        accounts = []
        for value in values:
            if isinstance(value, models.Account):
                self._account_map[value.key] = value
            else:
                value = models.Account.resolve_key(value)
                value = self._account_map.get(value, value)
            accounts.append(value)
        accounts = models.Account.resolve_list(accounts)
        for account in accounts:
            self._account_map[account.key] = account
        return accounts


class MutableStream(Stream):
    # Avoid accidentally setting unsupported attributes.
    __slots__ = ['account', '_autojoin_disabled']

    def __init__(self, me, stream, disable_autojoin=False, **kwargs):
        super(MutableStream, self).__init__(stream, **kwargs)
        self._autojoin_disabled = disable_autojoin
        self.account = models.Account.resolve(me)
        if not self.account:
            raise ValueError('Invalid account')
        if not self.participant and stream.service_content_id and not disable_autojoin:
            # The account is not a participant but may become one if they are whitelisted.
            s, t, content_id = models.Service.parse_identifier(stream.service_content_id)
            stream_team = (s, t)
            for identity in self.account.identifiers:
                try:
                    s, t, i = models.Service.parse_identifier(identity.id())
                except:
                    continue
                if (s, t) != stream_team:
                    continue
                # The special content id "*" means anyone can join the stream.
                if content_id != '*' and i not in stream.service_members:
                    continue
                # This account is allowed to join this stream.
                self._set_participants(add=[self.account])
                logging.debug('Auto-joined stream %d', stream.key.id())
                break
        if self.participant:
            self._account_map[self.account.key] = self.account

    @validate
    def announce_status(self, status, estimated_duration=None):
        if status != 'viewing-attachment' and len(self.participants) > 10:
            # Don't notify big groups of playback updates.
            return
        self.notify_others(notifs.ON_STREAM_STATUS, stream_id=self.key.id(),
                           status=status, estimated_duration=estimated_duration)

    @validate
    def bump_last_interaction(self, notify=True):
        self._tx(models.Stream.set_last_interaction, datetime.utcnow())
        if notify:
            self.notify_change()

    @validate
    def buzz(self):
        self.bump_last_interaction(notify=False)
        self.notify_change(notifs.ON_STREAM_BUZZ)
        self._report('buzzed')

    @validate
    def eject(self, participants, **kwargs):
        if not isinstance(participants, (list, set)):
            participants = [participants]
        account_keys = models.Account.resolve_keys(participants)
        if self.account.key in account_keys:
            raise errors.ForbiddenAction('Leave instead of removing from participants')
        self._set_participants(remove=account_keys, **kwargs)

    def for_participant(self, other_participant):
        if models.Account.resolve_key(other_participant) == self.account.key:
            return self
        return super(MutableStream, self).for_participant(other_participant,
            disable_autojoin=self._autojoin_disabled)

    def get_attachment(self, attachment_id):
        for attachment in self.attachments:
            if attachment.id == attachment_id:
                return attachment

    def get_others(self):
        return self.get_accounts(exclude_account=self.account)

    def get_relevant_chunks(self, num_chunks):
        """Picks the first `num_chunks` unplayed chunks or the most recent `num_chunks`
        played chunks (or a mix).
        """
        chunks = self.chunks
        # Find the first unplayed chunk index.
        for i, chunk in enumerate(chunks):
            if chunk.sender == self.account.key:
                # Ignore own chunks.
                continue
            if chunk.end > self.played_until:
                break
        else:
            i = len(chunks) - 1
        if i < 0:
            # There are fewer chunks than we wanted, no need to slice.
            return chunks
        # Grab a slice that contains the most relevant chunks.
        end = min(i + num_chunks, len(chunks))
        start = max(end - num_chunks, 0)
        return chunks[start:end]

    def hide(self):
        """Hides the stream from the user's recents list."""
        self._tx(models.Stream.set_visibility, hide_for=[self.account.key])
        self.notify_current(notifs.ON_STREAM_HIDDEN, stream_id=self.key.id(),
                            ignore_visibility=True)
        self._report('hide')

    @validate
    def invite(self, participants, **kwargs):
        # TODO: Decide on 1:1 restrictions.
        # TODO: We should probably send a request to the user instead of adding them.
        if not isinstance(participants, (list, set)):
            participants = [participants]
        participants = models.Account.resolve_keys(participants)
        # TODO: Blocking shouldn't stop the stream from existing, only prevent it from
        #       being visible to the blocker.
        for key in participants:
            if key in self.account.blocked_by:
                logging.info('Blocked Stream.invite from %s to %s', self.account.key, key)
                return
        self._set_participants(add=participants, **kwargs)

    def is_chunk_played(self, chunk):
        return chunk.sender == self.account.key or chunk.end <= self.played_until

    @property
    def is_played(self):
        return self.account.key not in self._stream.not_played_by

    @property
    def joined(self):
        if not self.participant:
            raise ValueError('Stream has no current participant')
        return self.participant.joined

    @property
    def last_chunk_end(self):
        if not self.participant:
            raise ValueError('Stream has no current participant')
        return self.participant.last_chunk_end

    @property
    def last_played_from(self):
        if not self.participant:
            raise ValueError('Stream has no current participant')
        return self.participant.last_played_from

    def leave(self, **kwargs):
        if not self.title and not self.shareable and len(self.participants) < 3:
            # Just hide 1:1 conversations.
            self.hide()
            return
        self._set_participants(remove=[self.account.key], **kwargs)

    def notify(self, event_type, **kwargs):
        # Notifies all participants in the stream of an event.
        for p in self._stream.participants:
            self._notify_one(p, event_type, **kwargs)

    def notify_change(self, event_type=notifs.ON_STREAM_CHANGE, **kwargs):
        # Notifies everyone else of the specified event and current account of a change.
        self.notify_current(notifs.ON_STREAM_CHANGE, add_stream=True)
        self.notify_others(event_type, add_stream=True, **kwargs)

    def notify_current(self, event_type, **kwargs):
        self._notify_one(self.participant, event_type, **kwargs)

    def notify_first_play(self, chunk, player=None):
        if player and player.key == chunk.sender:
            return
        play_count = sum(1 for p in self.participants if p.played_chunk(chunk))
        play_count += chunk.external_plays
        if play_count != 1:
            return
        self.notify_current(notifs.ON_STREAM_CHUNK_FIRST_PLAY,
                            add_stream=True, player=player)

    def notify_others(self, event_type, **kwargs):
        # Notifies all participants in the stream except the current account.
        for p in self._stream.participants:
            if p.account == self.account.key:
                continue
            self._notify_one(p, event_type, **kwargs)

    @property
    def others_played(self):
        if not self.has_chunks:
            return None
        played = None
        for p in self._stream.participants:
            if p.account == self.account.key or p.played_until < p.last_chunk_end:
                continue
            timestamp = p.played_until_changed
            if not played or timestamp < played:
                played = timestamp
        return played

    def others_played_until(self, ts):
        return [p for p in self._stream.participants
                if p.account != self.account.key and p.played_until >= ts]

    @property
    def participant(self):
        return self.get_participant(self.account)

    @property
    def played_until(self):
        if not self.participant:
            raise ValueError('Stream has no current participant')
        if not self.has_chunks:
            # If there is nothing to play, just return that the entire stream is played.
            return self.last_chunk_end
        return self.participant.played_until

    @property
    def presentation_title(self):
        if self.title:
            return self.title
        return ', '.join(a.display_name for a in self.get_others())

    def public(self, num_chunks=5, version=None, **kwargs):
        if not self.participant:
            raise errors.ForbiddenAction('Account is not a participant in stream')
        # Convert the stream to a dict, including the set account's metadata.
        data = {
            'chunks': self.get_relevant_chunks(num_chunks),
            'created': self.created,
            'id': self.key.id(),
            'joined': self.joined,
            'played_until': self.played_until,
            'title': self.title,
            'total_duration': self.total_duration,
            'visible': self.visible,
        }
        # Note: Versions before 6 included the current account in the participants list.
        participants = self.get_others() if version >= 6 else self.get_accounts()
        if version < 11:
            data['color'] = None
        if version >= 6:
            data['last_interaction'] = self.last_interaction
            data['last_played_from'] = self.last_played_from
            data['others'] = participants
        else:
            data['last_chunk_end'] = self.last_chunk_end
            data['participants'] = participants
        if version >= 7:
            data['image_url'] = self.image_url
        if 17 <= version < 22:
            data['service'] = None
        if version >= 18:
            data['attachments'] = {a.id: a for a in self.attachments}
            data['invite_token'] = self.invite_token
        elif version >= 16:
            data['attachments'] = self.attachments
        if version < 23:
            # This data is available with more info from Participant in v23+.
            data['others_listened'] = self.others_played
        if version >= 28:
            data['service_content_id'] = self.service_content_id
        if version >= 29:
            data['service_member_count'] = len(self.service_members) or None
        return data

    @validate
    def react_to_chunk(self, chunk_id, reaction_type):
        chunk_key = ndb.Key('Chunk', int(chunk_id), parent=self.key)
        stream, chunk = models.Stream.set_chunk_reaction(chunk_key, self.account.key,
                                                         reaction_type)
        if not stream:
            raise errors.ServerError('Failed to update stream (transaction rolled back)')
        self._stream = stream
        self._chunks = None
        self.notify(notifs.ON_STREAM_CHUNK_REACTION, add_stream=True, chunk=chunk)
        return chunk

    @validate
    def remove_attachment(self, attachment_id):
        self._tx(models.Stream.remove_attachment, self.account.key, attachment_id)
        self.notify(notifs.ON_STREAM_ATTACHMENT, stream_id=self.key.id(),
                    attachment_id=attachment_id, attachment=None)

    @validate
    def remove_chunk(self, chunk_id):
        # TODO: Only allow sender to remove their own chunk?
        try:
            chunk_id = int(chunk_id)
        except (TypeError, ValueError):
            raise errors.InvalidArgument('Invalid chunk id')
        self._tx(models.Stream.remove_chunk, chunk_id)
        # TODO: An event that makes client delete the chunk locally.

    @validate
    def send(self, payload, duration, client_id=None, export=False, mute_notification=False, text=None, **kwargs):
        if 'allow_duplicate' not in kwargs and self.account.status == 'bot':
            # Bots get to send duplicate content.
            kwargs['allow_duplicate'] = True
        if text:
            assert 'text_segments' not in kwargs
            segment = models.TextSegment(start=0, duration=duration, text=text)
            kwargs['text_segments'] = [segment]
        if self.account.location_info:
            kwargs['location'] = self.account.location_info.location
            kwargs['timezone'] = self.account.location_info.timezone
        if export and 'external_content_id' not in kwargs:
            # Create random content id first so it can be stored with the chunk.
            kwargs['external_content_id'] = models.ExportedContent.random_id()
        self._tx(models.Stream.add_chunk, self.account.key, payload, duration, **kwargs)
        chunk = self.chunks[-1]
        self.notify_change(notifs.ON_STREAM_CHUNK, chunk=chunk,
                           mute_notification=mute_notification)
        self._report('sent', duration=chunk.duration / 1000.0)
        if export or (self.service_content_id and not self.export_content_disabled):
            params = {
                'account_id': self.account.key.id(),
                'account_image_url': self.account.image_url or '',
                'account_name': self.account.display_name or '',
                'attachments': convert.to_json(chunk.attachments),
                'chunk_id': chunk.chunk_id,
                'client_id': client_id or '',
                'content_id': chunk.external_content_id or '',
                'destination': self.service_content_id or '',
                'duration': chunk.duration,
                'stream_id': self.key.id(),
                'stream_image_url': self.image_url or '',
                'stream_title': self.title or '',
                'text': convert.to_json(chunk.text_segments),
                'timestamp': convert.unix_timestamp_ms(chunk.start),
                'url': chunk.url,
            }
            taskqueue.add(url='/_ah/jobs/export_content', params=params,
                          queue_name=config.SERVICE_QUEUE_NAME)

    @property
    def service_content_id(self):
        if self._stream.service_content_id:
            # Assigned value takes precedence.
            return self._stream.service_content_id
        if self.export_content_disabled:
            return None
        others = self.get_others()
        if len(others) != 1 or self.title:
            # Only autogenerate service content id for 1:1s.
            return None
        # Get the other account's identifier for the first team in common.
        pairs = self.account.common_teams(others[0], exclude_services=['email'])
        try:
            _, identifier = next(pairs)
        except StopIteration:
            return None
        return identifier

    @validate
    def set_attachment(self, attachment_id, **kwargs):
        self._tx(models.Stream.set_attachment, self.account.key, attachment_id, **kwargs)
        self.notify(notifs.ON_STREAM_ATTACHMENT, stream_id=self.key.id(),
                    attachment_id=attachment_id,
                    attachment=self.get_attachment(attachment_id))

    @validate
    def set_featured(self, featured):
        if featured and (not self.title or not self.image):
            raise errors.ForbiddenAction('Stream needs an image and title to be featured')
        self._tx(models.Stream.set_featured, featured)
        # TODO: Notification?

    @validate
    def set_image(self, image):
        old_image = self.image
        self._tx(models.Stream.set_image, image)
        if self.image != old_image:
            self.notify_change(notifs.ON_STREAM_IMAGE, image_set=(image is not None))

    @validate
    def set_played_until(self, timestamp, report=True):
        # Convert the timestamp if necessary.
        if not isinstance(timestamp, datetime):
            try:
                timestamp = convert.from_unix_timestamp_ms(int(timestamp))
            except (TypeError, ValueError):
                raise errors.InvalidArgument('Invalid timestamp')
        if abs(timestamp - self.last_chunk_end) < timedelta(seconds=1):
            # Ignore sub-second differences between "played until" and end of stream.
            timestamp = self.last_chunk_end
        old_played_until = self.played_until
        was_unplayed = not self.is_played
        self._tx(models.Stream.set_played_until, self.participant.account, timestamp)
        if self.played_until != old_played_until:
            for chunk in self.chunks:
                if chunk.end <= old_played_until:
                    continue
                sender_stream = self.for_participant(chunk.sender)
                sender_stream.notify_first_play(chunk, player=self.account)
            self.notify_change(notifs.ON_STREAM_PLAY,
                               old_played_until=old_played_until,
                               played_until=self.played_until)
        if not report:
            return
        # Report the user updated their played state and seconds played.
        duration = sum((
            min(c.end, timestamp) - max(c.start, old_played_until)
            for c in self.chunks
            if c.sender != self.account.key
            and c.end > old_played_until and c.start < timestamp),
            timedelta())
        self._report('played', duration=duration.total_seconds(), unplayed=was_unplayed)

    @validate
    def set_shareable(self, shareable):
        self._tx(models.Stream.set_shareable, shareable)
        self.notify_change(notifs.ON_STREAM_SHAREABLE)

    @validate
    def set_title(self, title):
        old_title = self.title
        self._tx(models.Stream.set_title, title)
        if self.title != old_title:
            self.notify_change(notifs.ON_STREAM_TITLE)

    @validate
    def show(self):
        """Shows the stream in the user's recents list."""
        self._tx(models.Stream.set_visibility, show_for=[self.account.key])
        self.notify_current(notifs.ON_STREAM_SHOWN, stream_id=self.key.id())

    @property
    def visible(self):
        return self.account.key in self._stream.visible_by

    def _notify_one(self, participant, event_type, **kwargs):
        return self._notify_one_async(participant, event_type, **kwargs).get_result()

    @ndb.tasklet
    def _notify_one_async(self, participant, event_type, add_stream=False, ignore_visibility=False, **kwargs):
        if 'sender_id' in kwargs:
            raise ValueError('sender_id is included automatically')
        if 'stream' in kwargs:
            raise ValueError('Use add_stream=True to include stream')
        stream = self.for_participant(participant)
        if not stream.visible and not ignore_visibility:
            # Don't send notifs for invisible streams.
            return
        kwargs['sender_id'] = self.account.key.id()
        if add_stream:
            kwargs['stream'] = stream
        yield notifs.Hub(self.lookup_account(participant)).emit_async(event_type, **kwargs)

    def _report(self, status, **kwargs):
        # Create a default stream event.
        now = datetime.utcnow()
        event = events.StreamV3(
            self.account, status=status,
            stream_id=self.key.id(),
            stream_age=(now - self.created).total_seconds(),
            participant_ids=[k.id() for k in self.participant_keys],
            num_participants=len(self.participants),
            num_active_users=sum(a.is_active_user for a in self.get_accounts()),
            delta=(now - self.last_chunk_end).total_seconds(),
            has_active_user_content=any(self.lookup_account(c.sender).is_active_user
                                        for c in self.chunks
                                        if c.sender != self.account.key),
            featured=bool(self.featured > 0 or self.has_been_featured),
            unplayed=self.played_until < self.last_chunk_end)
        # Update the event with custom values.
        for k, v in kwargs.iteritems():
            setattr(event, k, v)
        event.report()
        # TODO: Remove everything below.
        if status == 'played' and not self.has_chunks:
            # Legacy condition which would otherwise crash below.
            return
        event = events.StreamV2(
            self.account.key.id(), status=status, stream_id=self.key.id(),
            participant_ids=[p.account.id() for p in self.participants],
            chunk_streak=0, unplayed=self.played_until < self.last_chunk_end)
        # Count number of chunks in a row by current user.
        for chunk in reversed(self.chunks):
            if chunk.sender != self.account.key:
                break
            event.chunk_streak += 1
        # Set duration when a chunk was sent or played.
        if status in ('played', 'sent'):
            event.duration = self.chunks[-1].duration / 1000.0
        if status == 'sent':
            # Delta is time between this chunk and previous one.
            # NOTE: This might be negative in some cases.
            if len(self.chunks) >= 2:
                prev_chunk, chunk = self.chunks[-2:]
                event.delta = (chunk.start - prev_chunk.end).total_seconds()
        elif self.has_chunks:
            # Delta is time between last chunk and now.
            event.delta = (now - self.chunks[-1].end).total_seconds()
        event.report()

    def _set_participants(self, add=None, remove=None, **kwargs):
        params = dict(kwargs, remove=remove)
        if add:
            add_accounts = self._lookup_accounts(add)
            params['add'] = add_accounts
            params['owners'] = {}
            # All new bots will be owned by the user who added them.
            owner = self.participant.owner if self.account.is_bot else self.account.key
            for account in add_accounts:
                if not account.is_bot:
                    continue
                params['owners'][account.key] = owner
        # Update participants list and create a diff based on the changes.
        before = models.Account.resolve_keys(self.participants)
        self._tx(models.Stream.set_participants, **params)
        after = models.Account.resolve_keys(self.participants)
        # TODO: Make a difference of leaving vs. kicking and inviting vs. joining?
        # Notify added accounts that they joined.
        added = after - before
        for key in added:
            self._notify_one(key, notifs.ON_STREAM_JOIN, stream_id=self.key.id())
            self._report('joined' if key == self.account.key else 'added-participant')
        # Notify removed accounts that they left.
        removed = before - after
        for key in removed:
            # TODO: Report removed participants.
            self._notify_one(key, notifs.ON_STREAM_LEAVE, stream_id=self.key.id(),
                             ignore_visibility=True)
        # Notify everyone else of the changes.
        added_ids, removed_ids = [k.id() for k in added], [k.id() for k in removed]
        if added_ids or removed_ids:
            for key in after & before:
                # TODO: Include participants in this notif.
                self._notify_one(key, notifs.ON_STREAM_PARTICIPANTS,
                                 added=added_ids, removed=removed_ids,
                                 stream_id=self.key.id())
        if self.account.key not in after:
            # The current account was removed from the stream.
            self.account = None
            self._stream = None
            self._chunks = None

    def _tx(self, method, *args, **kwargs):
        for _ in xrange(2):
            stream = method(self._stream.key, *args, **kwargs)
            if stream: break
        else:
            # If the loop falls through, stream was never set.
            raise errors.ServerError('Failed to update stream (transaction rolled back)')
        self._stream = stream
        self._chunks = None


class StreamsHandler(object):
    """Handles the streams for a particular account. The streams will be returned from
    the perspective of the account (meaning "joined", "played_until", etc. values will be
    for that account specifically.

    """
    def __init__(self, account):
        self.account = models.Account.resolve(account)
        if not self.account:
            raise ValueError('Could not resolve provided account')

    def get(self, others, all_chunks=False, create=False, disable_autojoin=False,
            reason='unknown', solo=False, title=None, **kwargs):
        try:
            accounts = models.Account.resolve_list([self.account] + others)
            if len(accounts) == 2 and accounts[0].key == accounts[1].key:
                # The user specified themselves as the second user so this should be solo.
                accounts = accounts[:1]
                solo = True
            if create:
                for a in accounts:
                    if a.key in self.account.blocked_by:
                        logging.info('Blocked stream creation by %s (with %s)',
                                     self.account.key, a.key)
                        raise errors.InvalidArgument('Could not get stream')
                assert 'owners' not in kwargs
                kwargs['owners'] = {a.key: self.account.key for a in accounts if a.is_bot}
                stream, new = models.Stream.get_or_create(title, accounts, solo=solo, **kwargs)
            else:
                stream, new = models.Stream.get(accounts, solo=solo, title=title), False
        except ValueError:
            raise errors.InvalidArgument('Got one or more invalid account(s)')
        if not stream:
            return None
        handler = MutableStream(self.account, stream,
                                account_map={a.key: a for a in accounts},
                                disable_autojoin=disable_autojoin)
        if new:
            # Notify all participants that a new stream including them was created.
            handler.notify(notifs.ON_STREAM_NEW, add_stream=True, reason=reason)
            handler._report('created')
        else:
            # The stream already existed - update properties.
            image = kwargs.get('image')
            if image:
                handler.set_image(image)
        if not new and all_chunks:
            handler.load_all_chunks()
        return handler

    def get_by_id(self, stream_id, all_chunks=False, **kwargs):
        try:
            if all_chunks:
                stream, chunks = models.Stream.get_by_id_with_chunks(int(stream_id))
            else:
                stream = models.Stream.get_by_id(int(stream_id))
                chunks = None
        except (TypeError, ValueError):
            raise errors.InvalidArgument('Invalid stream id')
        if not stream:
            raise errors.ResourceNotFound('That stream does not exist')
        return MutableStream(self.account, stream, chunks=chunks, **kwargs)

    def get_or_create(self, others, title=None, **kwargs):
        return self.get(others, create=True, title=title, **kwargs)

    def get_recent(self, max_results=10, cursor=None):
        q = models.Stream.query(models.Stream.visible_by == self.account.key)
        q = q.order(-models.Stream.last_interaction)
        # Fetch a page of streams with the cursor passed in to the API.
        start_cursor = Cursor(urlsafe=cursor)
        streams, next_cursor, more = q.fetch_page(max_results, start_cursor=start_cursor)
        # Batch fetch all accounts referred by the streams for efficiency.
        account_keys = set(p.account for s in streams for p in s.participants)
        lookup = dict(zip(account_keys, ndb.get_multi(account_keys)))
        # Return a list of 10 wrapped streams.
        streams = [MutableStream(self.account, s, account_map=lookup) for s in streams]
        return streams, next_cursor.urlsafe() if more else None

    def get_unplayed_count(self):
        threshold = datetime.utcnow() - config.CHUNK_MAX_AGE
        return models.Stream.query(models.Stream.not_played_by == self.account.key,
                                   models.Stream.visible_by == self.account.key,
                                   models.Stream.last_interaction > threshold).count()

    def join_service_content(self, service_content_id, autocreate=True, **kwargs):
        # Variable name should stay "service_content_id" to prevent duplicate in kwargs.
        try:
            service, team, resource = identifiers.parse_service(service_content_id)
            content_id = identifiers.build_service(service, team, resource)
        except:
            raise errors.InvalidArgument('Invalid service identifier')
        q = models.Stream.query(models.Stream.service_content_id == content_id)
        stream = q.get()
        if not stream:
            if not autocreate:
                raise errors.ResourceNotFound('Cannot join that stream')
            handler = self.get_or_create([], service_content_id=content_id, **kwargs)
            logging.debug('Created stream for %s (%r)', content_id, handler.title)
            return handler
        handler = Stream(stream).join(self.account, do_not_bump=True)
        if 'title' in kwargs and handler.title != kwargs['title']:
            handler.set_title(kwargs['title'])
        if not handler.visible:
            handler.show()
        logging.debug('Joined stream for %s (%r)', content_id, handler.title)
        members = kwargs.get('service_members')
        if members and set(handler.service_members) != set(members):
            handler._stream.service_members = members
            handler._stream.put()
            logging.debug('Updated member list of %s (%r)', content_id, handler.title)
        return handler

    def send(self, others, payload, duration, reason='unknown', title=None, **kwargs):
        stream = self.get_or_create(others, reason=reason, title=title)
        stream.send(payload, duration, **kwargs)
        return stream
