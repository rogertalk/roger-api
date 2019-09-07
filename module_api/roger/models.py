# -*- coding: utf-8 -*-

import base64
import collections
from datetime import date, datetime, timedelta
import hashlib
from itertools import chain, izip, tee
import logging
import pytz
import re
import struct
import urllib

from flask import has_request_context, request

from google.appengine.ext import deferred, ndb

from roger import config, files, localize, location, push_service
from roger_common import convert, errors, identifiers, random, security


class StatusMixin(object):
    """Utility methods for inspecting account status."""

    @property
    def can_activate(self):
        return self.status != 'waiting'

    @property
    def can_make_requests(self):
        return self.is_activated and self.status not in ('banned', 'deleted')

    @property
    def is_activated(self):
        return self.status in ('active', 'banned', 'bot', 'deleted', 'employee', 'inactive', 'unclaimed')

    @property
    def is_active(self):
        """An account that has been active recently."""
        return self.status in ('active', 'bot', 'employee')

    @property
    def is_active_user(self):
        """An account that is active but not a bot or employee."""
        return self.status == 'active'

    @property
    def is_bot(self):
        """A machine-controlled account."""
        return self.status == 'bot'

    @property
    def is_employee(self):
        """Whether the user is working at 47 Center, Inc."""
        return self.status == 'employee'

    @property
    def is_inactive(self):
        """An account that hasn't been active for a long time."""
        return self.status == 'inactive'

    @property
    def is_temporary(self):
        """An account that has never received anything nor logged in."""
        return self.status in ('requested', 'temporary')


class Account(ndb.Model, StatusMixin):
    admin = ndb.BooleanProperty(default=False, indexed=False)
    birthday = ndb.DateProperty()
    blocked_by = ndb.KeyProperty(kind='Account', repeated=True)
    callback_url = ndb.StringProperty(indexed=False)
    callback_version = ndb.IntegerProperty(indexed=False)
    content_count = ndb.IntegerProperty(default=0)
    content_reaction_count = ndb.IntegerProperty(default=0)
    created = ndb.DateTimeProperty(auto_now_add=True)
    disable_welcome = ndb.BooleanProperty(indexed=False)
    first_chunk_played = ndb.BooleanProperty(default=True)
    follower_count = ndb.IntegerProperty(default=0)
    following_count = ndb.IntegerProperty(default=0)
    gender = ndb.StringProperty()
    greeting = ndb.StringProperty(indexed=False)
    greeting_duration = ndb.IntegerProperty(indexed=False)
    has_roger = ndb.BooleanProperty()
    identifiers = ndb.KeyProperty('aliases', indexed=False, repeated=True)
    image_url_ = ndb.StringProperty('image_url', indexed=False)
    invite_token = ndb.StringProperty(indexed=False)
    last_active = ndb.DateProperty(auto_now_add=True, default=date(2016, 2, 3))
    last_active_client = ndb.StringProperty(indexed=False)
    location_info = ndb.LocalStructuredProperty(location.LocationInfo)
    premium = ndb.BooleanProperty(default=False)
    premium_properties = ndb.StringProperty(indexed=False, repeated=True)
    primary_set = ndb.BooleanProperty(default=True, indexed=False)
    properties = ndb.JsonProperty()
    publish_ = ndb.BooleanProperty('publish', indexed=False)
    quality_ = ndb.IntegerProperty('quality')
    scheduled_welcome_emails = ndb.BooleanProperty(default=False)
    share_location = ndb.BooleanProperty(default=True, indexed=False)
    status = ndb.StringProperty(required=True)
    stored_display_name = ndb.StringProperty('display_name', indexed=False)
    streak_count = ndb.IntegerProperty(default=0, indexed=False)
    streak_max = ndb.IntegerProperty(default=0, indexed=False)
    streak_time = ndb.DateTimeProperty(indexed=False)
    total_content_found = ndb.IntegerProperty(default=0)
    verified = ndb.BooleanProperty(default=False, indexed=False)
    wallet = ndb.KeyProperty(kind='Wallet')
    wallet_bonus = ndb.KeyProperty(kind='Wallet')
    will_receive_push = ndb.BooleanProperty(default=False, indexed=False)
    youtube_channel_id = ndb.StringProperty(indexed=False)
    youtube_channel_thumb_url = ndb.StringProperty(indexed=False)
    youtube_channel_title = ndb.StringProperty(indexed=False)
    youtube_channel_views = ndb.IntegerProperty()
    youtube_subs = ndb.IntegerProperty()
    youtube_subs_updated = ndb.DateTimeProperty(indexed=False)
    youtube_reaction_views = ndb.IntegerProperty()
    youtube_reaction_views_updated = ndb.DateTimeProperty(indexed=False)

    _team_tuple = collections.namedtuple('MissingServiceTeam', 'name image_url')

    def __str__(self):
        return '{} ({})'.format(
            self.key.id(),
            ', '.join(key.id() for key in self.identifiers))

    @property
    def account_id(self):
        return self.key.id()

    @classmethod
    @ndb.transactional
    def add_block(cls, account_key, blocked_by):
        account = account_key.get()
        if not blocked_by in account.blocked_by:
            account.blocked_by.append(blocked_by)
            account.put()
        return account

    @classmethod
    def add_vote_async(cls, account_key):
        return CounterShard.increment_async('total_votes_received', parent=account_key)

    def common_teams(self, other, exclude_services=[]):
        """Finds common teams between this account and another, and generates
        the complete service identifiers for the two accounts as tuples.
        """
        exclude_services = map(Service.resolve_key, exclude_services)
        for id_key in self.identifiers:
            try:
                team = Service.parse_team(id_key.id())
                assert team
            except:
                continue
            for other_id_key in other.identifiers:
                try:
                    other_team = Service.parse_team(other_id_key.id())
                    if other_team[0] in exclude_services:
                        continue
                    if other_team == team:
                        yield id_key.id(), other_id_key.id()
                except:
                    continue

    @classmethod
    def create(cls, status, birthday=None, display_name=None, gender=None, identifier=None, image_url=None, **kwargs):
        if not isinstance(status, basestring):
            raise errors.InvalidArgument('Account status must be a string')
        cls.validate_birthday(birthday)
        if display_name is not None and not isinstance(display_name, basestring):
            raise errors.InvalidArgument('Display name must be a string')
        cls.validate_gender(gender)
        kwargs.setdefault('properties', {})
        account = cls(stored_display_name=display_name,
                      birthday=birthday, gender=gender,
                      status=status, **kwargs)
        if identifier:
            identity = Identity.claim(identifier)
            if not identity:
                raise errors.AlreadyExists('That identifier is already in use')
            account.identifiers.append(identity.key)
        else:
            identity = None
        account.first_chunk_played = False
        account.image_url = image_url
        account.primary_set = bool(identity and account.username)
        account.put()
        if identity:
            identity.account = account.key
            identity.status = account.status
            identity.put()
        return account

    def get_display_name(self):
        return self.stored_display_name or self.username
    def set_display_name(self, value):
        self.stored_display_name = value
    display_name = property(get_display_name, set_display_name)

    @property
    def display_name_set(self):
        return bool(self.stored_display_name)

    def filter_services(self, service, team=None):
        resources = []
        for id_key in self.identifiers:
            try:
                s = Service.parse_identifier(id_key.id())
                if s.service.id() != service or (team and s.team != team):
                    continue
                resources.append(s)
            except:
                continue
        return resources

    @property
    def first_email(self):
        for id_key in self.identifiers:
            identifier, identifier_type = identifiers.parse(id_key.id())
            if identifier_type == identifiers.EMAIL:
                return identifiers.email(identifier)
        return None

    def get_auth_key(self, service_key, team_key=None):
        return ServiceAuth.resolve_key((self, service_key, team_key))

    @ndb.tasklet
    def get_balances_async(self):
        wallet, wallet_bonus = yield (
            self._get_or_create_wallet_async(),
            self._get_or_create_wallet_bonus_async())
        raise ndb.Return((wallet.balance, wallet_bonus.balance))

    def get_total_votes_received_async(self):
        return CounterShard.get_count_async('total_votes_received', parent=self.key)

    def get_weather_async(self):
        """Returns the weather for the account's location if available and shared."""
        if not self.location_info or not self.share_location:
            return None
        return self.location_info.get_weather_async()

    @property
    def greeting_url(self):
        return files.storage_url(self.greeting)

    def has_service(self, service):
        service_key = Service.resolve_key(service)
        for id_key in self.identifiers:
            try:
                s, _, _ = Service.parse_identifier(id_key.id())
            except:
                continue
            if s == service_key:
                return True
        return False

    @property
    def image_url(self):
        url = self.image_url_
        if not url:
            # The byte will be evenly distributed in range 0-255.
            byte = ord(hashlib.md5(str(self.key.id())).digest()[0])
            index = byte % len(config.DEFAULT_ACCOUNT_IMAGES)
            url = config.DEFAULT_ACCOUNT_IMAGES[index]
        return files.storage_url(url)

    @image_url.setter
    def image_url(self, value):
        self.image_url_ = files.storage_url(value)

    def is_on_team(self, service, team):
        team_key = ServiceTeam.resolve_key((service, team))
        service_key = team_key.parent()
        for id_key in self.identifiers:
            try:
                s, t, _ = Service.parse_identifier(id_key.id())
            except:
                continue
            if (s, t) == (service_key, team_key):
                return True
        return False

    @property
    def is_reachable(self):
        if not self.is_activated:
            # A user that was never activated will not receive push notifs.
            return False
        if self.username:
            # Users with usernames are assumed to be properly onboarded.
            return True
        if self.will_receive_push:
            # Short-circuit expensive operation below.
            return True
        # TODO: Don't rely on this lookup at all because it's expensive.
        has_device = Device.query(ancestor=self.key).count(limit=1) > 0
        if has_device and not self.will_receive_push:
            self.will_receive_push = True
            self.put()
        return has_device

    @property
    def is_reactioncam_supported(self):
        if not self.last_active_client:
            return True
        if 'iPad3,' in self.last_active_client:
            return False
        if 'iPhone5,' in self.last_active_client:
            return False
        return True

    @classmethod
    def parse_birthday(cls, birthday):
        if isinstance(birthday, date):
            return birthday
        if not isinstance(birthday, basestring):
            raise errors.InvalidArgument('Birthday must be a date or a string representing a date')
        try:
            birthday = datetime.strptime(birthday,  '%Y-%m-%d').date()
        except:
            raise errors.InvalidArgument('Birthday must be in format YYYY-MM-DD')
        return birthday

    def public(self, include_extras=False, include_identifiers=False, version=None, view_account=None, **kwargs):
        # Hide some information from blocked users.
        if view_account and self.key in view_account.blocked_by:
            viewer_can_see = False
        else:
            viewer_can_see = True
        # Add some information when looking at own account.
        is_me = bool(view_account and view_account.key == self.key)
        result = {
            'id': self.key.id(),
            'display_name': self.display_name or 'Someone',
            'image_url': self.image_url,
            'status': self.status,
            'username': self.username or str(self.key.id()),
        }
        if 1 <= version and (is_me or include_identifiers):
            result['identifiers'] = [identity.id() for identity in self.identifiers]
        if 1 <= version and is_me:
            result['display_name_set'] = self.display_name_set
        if 1 <= version < 34 and include_extras:
            result['greeting'] = None
        if 1 <= version < 54 and is_me:
            result['services'] = self._service_info_async(version)
        if 1 <= version < 55 and is_me:
            result['share_location'] = self.share_location
        if 1 <= version < 55 and view_account:
            # Teams that this account has in common with the viewing account.
            if viewer_can_see:
                result['teams'] = list(set(i for i, _ in self.common_teams(view_account)))
            else:
                result['teams'] = []
            # Make sure that this user is sharing their location.
            if self.location_info and self.share_location and viewer_can_see:
                info = self.location_info
            else:
                info = location.LocationInfo()
            # Add location information into the API response.
            flag = location.flags.get(info.country)
            if flag:
                city = u'%s %s' % (info.city, flag)
            else:
                city = info.city
            result['location'] = city
            result['timezone'] = info.timezone
        if 36 <= version < 55:
            result['premium'] = self.premium
        if 37 <= version and is_me:
            result['onboarded'] = self.primary_set
        if 39 <= version and include_extras:
            result['properties'] = self.properties or {}
        if 41 <= version:
            result['follower_count'] = self.follower_count
            result['following_count'] = self.following_count
            if include_extras:
                if not view_account or is_me:
                    result['is_following'] = False
                else:
                    result['is_following'] = AccountFollow.is_following_async(view_account.key, self.key)
        if 42 <= version < 55 and include_extras:
            # In version 53+ this value is only returned for the current user.
            if version < 53 or is_me:
                result['total_votes_received'] = self.get_total_votes_received_async()
        if 43 <= version:
            result['content_count'] = self.content_count
        if 44 <= version:
            result['verified'] = self.verified
        if 48 <= version and is_me and include_extras:
            future = self.get_balances_async()
            result['balance'] = lambda: future.get_result()[0]
            result['bonus'] = lambda: future.get_result()[1]
        if 49 <= version:
            if self.properties and 'tiers' in self.properties:
                result['has_rewards'] = len(self.properties['tiers']) > 0
            else:
                result['has_rewards'] = False
        if 51 <= version and view_account:
            result['is_blocked'] = (view_account.key in self.blocked_by)
        if 52 <= version and is_me and include_extras:
            if self.birthday:
                result['birthday'] = self.birthday.strftime('%Y-%m-%d')
            else:
                result['birthday'] = None
            result['gender'] = self.gender
        if 54 <= version and is_me and include_extras:
            # TODO: Unset youtube_channel_id once account loses "youtube" service.
            if self.youtube_channel_id and self.has_service('youtube'):
                youtube = {
                    'id': self.youtube_channel_id,
                    'thumb_url': self.youtube_channel_thumb_url,
                    'title': self.youtube_channel_title or self.youtube_channel_id,
                }
            else:
                youtube = None
            result['youtube'] = youtube
        if 55 <= version and is_me and include_extras:
            result['premium_properties'] = self.premium_properties or []
        return result

    @property
    def publish(self):
        return self.quality > 1

    @property
    def quality(self):
        if self.quality_ is not None:
            return self.quality_
        if self.verified:
            return 4
        elif self.publish_ is True:
            return 2
        elif self.publish_ is None:
            return 1
        elif self.publish_ is False:
            return 0
        raise ValueError('Unknown quality state')

    @quality.setter
    def quality(self, value):
        self.quality_ = value

    @property
    def quality_has_been_set(self):
        return self.quality_ is not None

    def rebuild_identifiers(self):
        # Find all potential new identifiers pointing at the account.
        identifiers = {i.key: i for i in Identity.query(Identity.account == self.key)}
        # Perform an integrity check in a transaction.
        @ndb.transactional(xg=True)
        def integrity_check(account_key):
            account = account_key.get()
            identifiers.update((i.key, i) for i in ndb.get_multi(account.identifiers) if i)
            # Remove identifiers that no longer point at account.
            new_identifiers = list(identifiers)
            for key, identifier in identifiers.iteritems():
                if identifier.account != account_key:
                    new_identifiers.remove(key)
            account.identifiers = new_identifiers
            account.put()
            return account
        self.populate(**integrity_check(self.key).to_dict())

    @classmethod
    @ndb.transactional
    def remove_block(cls, account_key, blocked_by):
        account = account_key.get()
        try:
            account.blocked_by.remove(blocked_by)
            account.put()
        except ValueError:
            pass
        return account

    @classmethod
    def resolve(cls, value):
        return cls.resolve_async(value).get_result()

    @classmethod
    @ndb.tasklet
    def resolve_async(cls, value):
        if isinstance(value, cls):
            raise ndb.Return(value)
        key = yield cls.resolve_key_async(value)
        if key:
            logging.debug('Loaded accounts: %s', key.id())
        else:
            raise ndb.Return(None)
        account = yield key.get_async()
        raise ndb.Return(account)

    @classmethod
    def resolve_key(cls, value):
        return cls.resolve_key_async(value).get_result()

    @classmethod
    @ndb.tasklet
    def resolve_key_async(cls, value):
        if isinstance(value, cls):
            raise ndb.Return(value.key)
        elif isinstance(value, ndb.Key) and value.kind() == cls._get_kind():
            raise ndb.Return(value)
        elif isinstance(value, Participant):
            raise ndb.Return(value.account)
        elif isinstance(value, (int, long)):
            raise ndb.Return(ndb.Key(cls, value))
        elif isinstance(value, basestring):
            value, identifier_type = identifiers.parse(value)
            logging.debug('Looking up identifier %r (%s)', value, identifier_type)
            if identifier_type == identifiers.ACCOUNT_ID:
                if value <= 0:
                    raise ndb.Return(None)
                raise ndb.Return(ndb.Key(cls, value))
            if not value:
                raise ndb.Return(None)
            identity = yield Identity.get_by_id_async(value)
            if not identity:
                raise ndb.Return(None)
            raise ndb.Return(identity.account)

    @classmethod
    def resolve_keys(cls, value_list):
        futures = [cls.resolve_key_async(v) for v in value_list]
        keys = {f.get_result() for f in futures}
        if not all(keys):
            raise ValueError('Failed to resolve input list to account keys')
        return keys

    @classmethod
    @ndb.tasklet
    def resolve_keys_async(cls, value_list):
        futures = [cls.resolve_key_async(v) for v in value_list]
        keys = yield futures
        keys = set(keys)
        if not all(keys):
            raise ValueError('Failed to resolve input list to account keys')
        raise ndb.Return(keys)

    @classmethod
    def resolve_list(cls, value_list):
        return cls.resolve_list_async(value_list).get_result()

    @classmethod
    @ndb.tasklet
    def resolve_list_async(cls, value_list):
        results = list(value_list)
        missing = []
        indexes = []
        for index, value in enumerate(value_list):
            if not isinstance(value, cls):
                future = cls.resolve_key_async(value)
                missing.append(future)
                indexes.append(index)
        for index, future in enumerate(missing):
            key = yield future
            if not key:
                raise ValueError('Failed to resolve input list to accounts')
            missing[index] = key
        accounts = yield ndb.get_multi_async(missing)
        while indexes:
            results[indexes.pop()] = accounts.pop()
        if accounts or not all(results):
            raise ValueError('Failed to resolve input list to accounts')
        if missing:
            logging.debug('Loaded accounts: %s', ', '.join(str(k.id()) for k in missing))
        raise ndb.Return(results)

    @property
    def service_identifiers(self):
        tuples = []
        for id_key in self.identifiers:
            try:
                tuples.append(Service.parse_identifier(id_key.id()))
            except:
                continue
        return tuples

    def set_location(self, point, defer=True, **kwargs):
        if self.location_info and self.location_info.distance_km(point) < 100:
            return
        if defer:
            # TODO: Consider using a dedicated queue to avoid rate limiting.
            deferred.defer(Account._deferred_update_location, self.key, point,
                           _queue=config.LOCATION_QUEUE_NAME, **kwargs)
            return
        self._actually_set_location(point, **kwargs)

    @classmethod
    @ndb.transactional
    def set_primary_identifier(cls, account_key, primary_identifier):
        account = account_key.get()
        for index, identity in enumerate(account.identifiers):
            if identity.id() == primary_identifier:
                break
        else:
            raise errors.ForbiddenAction('The identifier belongs to another account')
        if index == 0:
            return account
        account.identifiers.insert(0, account.identifiers.pop(index))
        account.put()
        return account

    @classmethod
    @ndb.transactional(xg=True)
    def set_status(cls, account_key, new_status):
        account = account_key.get()
        account.status = new_status
        # Also update all Identity entities to hold the new status.
        identities = [Identity(key=key, account=account_key, status=new_status)
                      for key in account.identifiers]
        ndb.put_multi([account] + identities)
        return account

    def to_dict(self, include=None, exclude=None):
        # Avoid including certain deprecated properties in to_dict.
        unsupported_properties = [
            'birth_year',
            'has_roger',
            'services',
            'team_name',
            'temporary_password',
            'total_votes_received',
        ]
        exclude = unsupported_properties + (exclude or [])
        return super(Account, self).to_dict(include=include, exclude=exclude)

    @property
    def username(self):
        for identity in self.identifiers:
            _, identifier_type = identifiers.parse(identity.id())
            if identifier_type == identifiers.USERNAME:
                return identity.id()
        return None

    @classmethod
    def validate_birthday(cls, birthday, allow_none=True):
        if allow_none and birthday is None:
            return
        if not isinstance(birthday, date):
            raise errors.InvalidArgument('Birthday must be a date')
        if birthday.year < 1900 or birthday.year > datetime.utcnow().year:
            raise errors.InvalidArgument('Impossible birth year provided')

    @classmethod
    def validate_gender(cls, gender, allow_none=True):
        if allow_none and gender is None:
            return
        if not isinstance(gender, basestring):
            raise errors.InvalidArgument('Gender must be a string')
        if gender not in ('female', 'male', 'other'):
            raise errors.InvalidArgument('Unsupported gender value - please use "other" for now')

    def _actually_set_location(self, point, **kwargs):
        old = self.location_info
        if old and old.timestamp.date() == date.today() and old.distance_km(point) <= 1:
            return  # No update needed.
        info = location.LocationInfo.from_point(point, **kwargs)
        if not info:
            return  # We failed to determine the location.
        self.location_info = info
        self.put()

    @classmethod
    def _deferred_update_location(cls, account, point, **kwargs):
        logging.debug('Updating location in deferred callback')
        account = cls.resolve(account)
        account._actually_set_location(point, **kwargs)

    @ndb.tasklet
    def _get_or_create_wallet_async(self):
        if self.wallet:
            wallet = yield self.wallet.get_async()
            raise ndb.Return(wallet)
        _, wallet = yield Wallet.create_async(self.key)
        raise ndb.Return(wallet)

    @ndb.tasklet
    def _get_or_create_wallet_bonus_async(self):
        if self.wallet_bonus:
            wallet = yield self.wallet_bonus.get_async()
            raise ndb.Return(wallet)
        _, wallet = yield Wallet.create_async(self.key, attr='wallet_bonus')
        raise ndb.Return(wallet)

    @ndb.tasklet
    def _service_info_async(self, version):
        identifiers = yield tuple(self._service_info_one_async(si, version)
                                  for si in self.service_identifiers)
        raise ndb.Return(list(identifiers))

    @classmethod
    @ndb.tasklet
    def _service_info_one_async(cls, service_identifier, version):
        service_key, team_key, resource = service_identifier
        if team_key:
            service, team = yield service_key.get_async(), team_key.get_async()
            if not team:
                logging.error('Missing team: %s (%s)', team_key.id(), service_key.id())
                team = cls._team_tuple(name=team_key.id(), image_url=None)
            team_info = {'id': team_key.id(), 'name': team.name}
            if version >= 29:
                team_info['image_url'] = team.image_url
        else:
            service = yield service_key.get_async()
            team_info = None
        info = {
            'id': service_key.id(),
            'title': service.title,
            'image_url': service.image_url,
            'team': team_info,
        }
        if version >= 31:
            info['resource'] = resource
        raise ndb.Return(info)

    def _set_attributes(self, attrs):
        if 'display_name' in attrs:
            attrs['stored_display_name'] = attrs.pop('display_name')
        if 'image_url' in attrs:
            attrs['image_url_'] = files.storage_url(attrs.pop('image_url'))
        super(Account, self)._set_attributes(attrs)


class AccountEvent(ndb.Model):
    timestamp = ndb.DateTimeProperty(required=True)
    properties = ndb.JsonProperty(repeated=True)

    @classmethod
    def create(cls, *args, **kwargs):
        return cls.create_async(*args, **kwargs).get_result()

    @classmethod
    def create_async(cls, account_key, name, timestamp=None, **kwargs):
        if not timestamp:
            timestamp = datetime.utcnow()
        item = AccountEventItem(timestamp, name, **kwargs)
        return cls.create_batch_async(account_key, [item])

    @classmethod
    def create_batch(cls, *args, **kwargs):
        return cls.create_batch_async(*args, **kwargs).get_result()

    @classmethod
    def create_batch_async(cls, account_key, items):
        assert len(items) > 0, 'Provide at least one item'
        items = sorted(items, key=lambda i: i.timestamp)
        timestamp = items[0].timestamp
        event_id = convert.unix_timestamp_ms(timestamp)
        event = cls(id=event_id, timestamp=timestamp,
                    properties=[i.to_internal() for i in items],
                    parent=account_key)
        return event.put_async()

    @property
    def items(self):
        if 'name' in self._values:
            item = AccountEventItem(self.timestamp, ndb.GenericProperty('name')._get_value(self))
            item.client = ndb.GenericProperty('client')._get_value(self)
            item.event_class = ndb.GenericProperty('event_class')._get_value(self)
            item.properties = self.properties[0]
            return [item]
        if len(self.properties) == 1 and '_ts' not in self.properties[0]:
            props = dict(self.properties[0], _ts=convert.unix_timestamp_ms(self.timestamp))
            return [AccountEventItem.from_internal(props)]
        return map(AccountEventItem.from_internal, self.properties)


class AccountEventItem(object):
    __slots__ = ['client', 'event_class', 'name', 'properties', 'repeats', 'repeats_began', 'timestamp']

    def __init__(self, timestamp, name, client=None, event_class=None, properties=None):
        assert timestamp, 'Must provide event timestamp'
        assert name, 'Must provide event name'
        if not client and has_request_context():
            client = request.headers.get('User-Agent')
        self.client = client
        self.event_class = event_class
        self.name = name
        self.properties = dict(properties) if properties else {}
        self.repeats = 1
        self.repeats_began = timestamp
        self.timestamp = timestamp

    @classmethod
    def from_internal(cls, properties):
        properties = dict(properties)
        client = properties.pop('_ct', None)
        event_class = properties.pop('_cl', None)
        name = properties.pop('_nm', None)
        timestamp_ms = properties.pop('_ts', 0)
        timestamp = convert.from_unix_timestamp_ms(timestamp_ms)
        repeats = properties.pop('_rc', 1)
        repeats_began = convert.from_unix_timestamp_ms(properties.pop('_rt', timestamp_ms))
        item = cls(timestamp, name, client=client, event_class=event_class, properties=properties)
        item.repeats = repeats
        item.repeats_began = repeats_began
        return item

    def increment_repeats(self, timestamp):
        self.repeats += 1
        self.timestamp = timestamp

    def is_repeat_of(self, other):
        t = lambda i: (i.client, i.event_class, i.name, i.properties)
        return t(self) == t(other)

    def public(self, version=None, **kwargs):
        return {
            'client': self.client,
            'timestamp': self.timestamp,
            'class': self.event_class,
            'name': self.name,
            'properties': self.properties,
            'repeats': self.repeats,
            'repeats_began': self.repeats_began,
        }

    def to_internal(self):
        assert self.name
        properties = dict(self.properties)
        properties['_cl'] = self.event_class
        properties['_ct'] = self.client
        properties['_nm'] = self.name
        properties['_ts'] = convert.unix_timestamp_ms(self.timestamp)
        if self.repeats > 1:
            properties['_rc'] = self.repeats
            properties['_rt'] = convert.unix_timestamp_ms(self.repeats_began)
        return properties


class AccountFollow(ndb.Model):
    account = ndb.KeyProperty(Account)
    timestamp = ndb.DateTimeProperty(auto_now_add=True)

    @classmethod
    def fetch_followers_page(cls, *args, **kwargs):
        return cls.fetch_followers_page_async(*args, **kwargs).get_result()

    @classmethod
    @ndb.tasklet
    def fetch_followers_page_async(cls, b_key, page_size, keys_only=False, **q_options):
        q = cls.query(cls.account == b_key)
        q = q.order(-cls.timestamp)
        results, cursor, more = yield q.fetch_page_async(page_size, keys_only=True, **q_options)
        results = [k.parent() for k in results]
        if not keys_only:
            results = yield ndb.get_multi_async(results)
        raise ndb.Return((results, cursor, more))

    @classmethod
    def fetch_following_page(cls, *args, **kwargs):
        return cls.fetch_following_page_async(*args, **kwargs).get_result()

    @classmethod
    @ndb.tasklet
    def fetch_following_page_async(cls, a_key, page_size, keys_only=False, **q_options):
        q = cls.query(ancestor=a_key)
        q = q.order(-cls.timestamp)
        results, cursor, more = yield q.fetch_page_async(page_size, keys_only=True, **q_options)
        results = [ndb.Key(Account, k.id()) for k in results]
        if not keys_only:
            results = yield ndb.get_multi_async(results)
        raise ndb.Return((results, cursor, more))

    @classmethod
    @ndb.tasklet
    def follow_async(cls, a_key, b_keys):
        a, followed_b_keys = yield cls._create_and_increment_follows(a_key, list(b_keys))
        if not a:
            raise ndb.Return((None, []))
        futures = [cls._update_following(b_key, 1) for b_key in followed_b_keys]
        b_tuple = yield tuple(futures)
        b_list = list(b_tuple)
        missing = [k for k, b in zip(followed_b_keys, b_list) if not b]
        if missing:
            logging.error('%d tried to follow missing account(s) %r', a_key.id(), missing)
            yield tuple(cls.unfollow_async(a_key, b_key) for b_key in missing)
            b_list = filter(None, b_list)
        raise ndb.Return((a, b_list))

    @classmethod
    @ndb.tasklet
    def is_following_async(cls, a_key, b_key):
        key = ndb.Key(cls, b_key.id(), parent=a_key)
        result = yield key.get_async()
        raise ndb.Return(result != None)

    @classmethod
    @ndb.tasklet
    def unfollow_async(cls, a_key, b_key):
        a = yield cls._delete_and_decrement_follow(a_key, b_key)
        b = (yield cls._update_following(b_key, -1)) if a else None
        raise ndb.Return((a, b))

    @classmethod
    @ndb.transactional_tasklet
    def _create_and_increment_follows(cls, a_key, b_keys):
        keys = [ndb.Key(cls, b_key.id(), parent=a_key) for b_key in b_keys]
        follows = yield ndb.get_multi_async([a_key] + keys)
        a = follows.pop(0)
        if not a:
            raise ndb.Return((None, []))
        new_follows = []
        for i, f in enumerate(follows):
            if f:
                continue
            new_follows.append(cls(key=keys[i], account=b_keys[i]))
            a.following_count += 1
        if not new_follows:
            raise ndb.Return((None, []))
        yield ndb.put_multi_async([a] + new_follows)
        raise ndb.Return((a, [f.account for f in new_follows]))

    @classmethod
    @ndb.transactional_tasklet
    def _delete_and_decrement_follow(cls, a_key, b_key):
        key = ndb.Key(cls, b_key.id(), parent=a_key)
        a, follow = yield ndb.get_multi_async([a_key, key])
        if not a or not follow:
            raise ndb.Return(None)
        a.following_count -= 1
        yield a.put_async(), follow.key.delete_async()
        raise ndb.Return(a)

    @classmethod
    @ndb.transactional_tasklet
    def _update_following(cls, b_key, delta):
        # TODO: Replace with sharded counters if a user may get >1 follow/sec.
        b = yield b_key.get_async()
        if not b:
            raise ndb.Return(None)
        b.follower_count += delta
        yield b.put_async()
        raise ndb.Return(b)


class AccountNotification(ndb.Model):
    group_id = ndb.StringProperty()
    group_count = ndb.IntegerProperty(default=1, indexed=False)
    group_history = ndb.JsonProperty(repeated=True)
    properties = ndb.JsonProperty()
    seen = ndb.BooleanProperty(default=False)
    seen_timestamp = ndb.DateTimeProperty(indexed=False)
    timestamp = ndb.DateTimeProperty(auto_now_add=True)
    type = ndb.StringProperty(indexed=False, required=True)

    def __init__(self, group_key=None, group_history_max=5, group_history_keys=[], *args, **kwargs):
        self.group_key = group_key
        self.group_history_max = group_history_max
        self.group_history_keys = group_history_keys or []
        super(AccountNotification, self).__init__(*args, **kwargs)

    @classmethod
    def count_unseen(cls, account):
        return cls.count_unseen_async(account).get_result()

    @classmethod
    @ndb.tasklet
    def count_unseen_async(cls, account_key):
        # TODO: Consider changing this once the client can paginate notifs.
        q = cls.recent_query(account_key)
        notifs = yield q.fetch_async(50, projection=[cls.seen])
        raise ndb.Return(sum(0 if n.seen else 1 for n in notifs))

    @classmethod
    def new(cls, account_key, type, **kwargs):
        assert 'group_id' not in kwargs
        assert 'group_count' not in kwargs
        assert 'group_history' not in kwargs
        assert 'id' not in kwargs
        assert 'seen' not in kwargs
        assert 'seen_timestamp' not in kwargs
        assert 'timestamp' not in kwargs
        assert isinstance(account_key, ndb.Key) and account_key.kind() == 'Account'
        kwargs.setdefault('properties', {})
        return cls(type=type, parent=account_key, **kwargs)

    def public(self, version=None, **kwargs):
        result = dict(self.properties,
            id=self.key.id(),
            seen=self.seen,
            timestamp=self.timestamp,
            type=self.type)
        if version >= 42:
            result['group_count'] = self.group_count
            result['group_history'] = self.group_history[1:]
        return result


    @classmethod
    def put_grouped_async(cls, notif):
        # Set the group_key property of a notif to group it.
        assert notif.group_key is not None, 'group_key must not be None'
        assert notif.group_history_max is not None, 'group_history_max must not be None'
        assert notif.group_history_keys is not None, 'group_history_keys must not be None'
        # Ensure that this notif isn't already grouped.
        assert notif.group_id is None, 'group_id must be None'
        notif.group_id = '{}:{}'.format(notif.type, notif.group_key)
        return cls._put_grouped_async(notif, notif.group_history_max, notif.group_history_keys)

    @classmethod
    def recent_query(cls, account_key):
        assert isinstance(account_key, ndb.Key) and account_key.kind() == 'Account'
        q = cls.query(ancestor=account_key)
        q = q.order(-cls.timestamp)
        return q

    @classmethod
    @ndb.transactional_tasklet
    def _put_grouped_async(cls, notif, history_max, history_keys):
        # Look up any existing notification for the grouping id.
        q = cls.query(cls.group_id == notif.group_id, ancestor=notif.key.parent())
        existing = yield q.get_async()
        if existing:
            # Update existing notif object instead of creating a new one.
            existing.group_count += 1
            existing.properties = notif.properties
            existing.timestamp = notif.timestamp or datetime.utcnow()
            notif = existing
        # Create a subset of properties that will be available for the last few notifs.
        props = {k: notif.properties[k] for k in notif.properties if k in history_keys}
        if props and history_max > 0:
            history = notif.group_history[:history_max - 1]
            notif.group_history = [props] + history
        # Update notif to be unseen with new data.
        notif.properties = notif.properties
        notif.seen = False
        yield notif.put_async()
        raise ndb.Return(notif)


class Attachment(ndb.Expando):
    id = ndb.StringProperty(required=True)
    type = ndb.StringProperty(required=True)
    account = ndb.KeyProperty(Account, required=True)
    timestamp = ndb.DateTimeProperty(required=True)

    def __setattr__(self, name, value):
        if (not isinstance(value, (dict, list)) or
            name.startswith('_') or
            isinstance(getattr(self.__class__, name, None), (ndb.Property, property))):
            return super(Attachment, self).__setattr__(name, value)
        self._clone_properties()
        prop = ndb.LocalStructuredProperty(ndb.Expando, name, repeated=isinstance(value, list))
        prop._code_name = name
        self._properties[name] = prop
        prop._set_value(self, value)

    def public(self, version=None, **kwargs):
        def to_dict(value, **kwargs):
            if isinstance(value, ndb.Expando):
                value = value.to_dict(**kwargs)
            if isinstance(value, list):
                return map(to_dict, value)
            elif isinstance(value, dict):
                for k, v in value.iteritems():
                    value[k] = to_dict(v)
            return value
        if version < 18:
            return to_dict(self, exclude=['account', 'timestamp'])
        elif version < 22:
            return to_dict(self, exclude=['account', 'id', 'timestamp'])
        return dict(to_dict(self, exclude=['account', 'id']),
                    account_id=self.account.id())


class AuthCode(ndb.Model):
    account = ndb.KeyProperty(Account)
    client_id = ndb.StringProperty(indexed=False, required=True)
    expires = ndb.DateTimeProperty(required=True)
    redirect_uri = ndb.StringProperty(indexed=False)

    def public(self, **kwargs):
        return {
            'code': self.key.id(),
            'redirect_uri': self.redirect_uri,
        }


class Challenge(ndb.Model):
    """
    Data structure for verifying a code challenge.

    The key name will be the identifier (e.g., a phone number) that the challenge is for.
    """
    SUCCESS = 0
    INVALID_SECRET = 1
    TOO_MANY_ATTEMPTS = 2
    EXPIRED = 3

    code = ndb.StringProperty(indexed=False)
    hash = ndb.BlobProperty(indexed=False)
    salt = ndb.BlobProperty(indexed=False)
    tries = ndb.IntegerProperty(default=0, indexed=False)
    updated = ndb.DateTimeProperty(indexed=False)

    @property
    def is_expired(self):
        # TODO there are corner cases where user may request the challenge close to expire
        #      time leading to failure in the response
        return datetime.utcnow() - self.updated > config.CHALLENGE_TTL

    @classmethod
    @ndb.transactional
    def get(cls, client, identifier, code_length):
        key = cls.resolve_key((client, identifier))
        challenge = key.get()
        if not challenge or challenge.is_expired:
            code = ''
            for _ in xrange(code_length):
                code += random.choice('0123456789')
            challenge = cls(key=key, code=code, updated=datetime.utcnow())
            challenge.put()
        return challenge

    @classmethod
    def resolve_key(cls, value):
        if isinstance(value, cls):
            return value.key
        elif isinstance(value, ndb.Key) and value.kind() == cls._get_kind():
            return value
        elif isinstance(value, basestring):
            return ndb.Key(cls, value)
        elif isinstance(value, (list, tuple)):
            return ndb.Key(cls, '{}:{}'.format(*value))
        raise TypeError('Expected a Challenge key')

    @classmethod
    @ndb.transactional
    def validate(cls, client, identifier, secret):
        logging.debug('Validating secret: %r', secret)
        challenge = cls.resolve_key((client, identifier)).get()
        if not challenge or challenge.is_expired:
            logging.debug('Challenge expired')
            return cls.EXPIRED
        if challenge.code == secret or challenge.hash == security.salted_sha256(secret, challenge.salt or ''):
            challenge.key.delete()
            return cls.SUCCESS
        challenge.tries += 1
        if challenge.tries >= config.CHALLENGE_MAX_TRIES:
            logging.debug('Challenge failed (deleted after %d attempts)', challenge.tries)
            challenge.key.delete()
            return cls.TOO_MANY_ATTEMPTS
        else:
            logging.debug('Challenge failed (attempt %d/%d)', challenge.tries, config.CHALLENGE_MAX_TRIES)
            challenge.put()
        return cls.INVALID_SECRET


class ChunkAttachment(ndb.Model):
    title = ndb.StringProperty(indexed=False)
    url = ndb.StringProperty(indexed=False, required=True)

    def public(self, version=None, **kwargs):
        return {'title': self.title, 'url': self.url}


class TextSegment(ndb.Model):
    duration = ndb.IntegerProperty(indexed=False, required=True)
    start = ndb.IntegerProperty(indexed=False, required=True)
    text = ndb.StringProperty(indexed=False, required=True)

    def public(self, version=None, **kwargs):
        return {
            'duration': self.duration,
            'start': self.start,
            'text': self.text,
        }


class Chunk(ndb.Model):
    attachments = ndb.LocalStructuredProperty(ChunkAttachment, repeated=True)
    end = ndb.DateTimeProperty(required=True)
    external_content_id = ndb.StringProperty(indexed=False)
    external_plays = ndb.IntegerProperty(default=0, indexed=False)
    location = ndb.GeoPtProperty()
    payload = ndb.StringProperty(indexed=False, required=True)
    persist = ndb.BooleanProperty(default=False)
    sender = ndb.KeyProperty(Account, required=True)
    start = ndb.DateTimeProperty(required=True)
    reaction_keys = ndb.KeyProperty('reactions', Account, repeated=True)
    reaction_types = ndb.StringProperty(indexed=False, repeated=True)
    text_segments = ndb.LocalStructuredProperty(TextSegment, repeated=True)
    timezone = ndb.StringProperty(indexed=False)
    token = ndb.StringProperty()

    @property
    def duration(self):
        delta = (self.end - self.start) / 1000
        return delta.seconds * 1000000 + delta.microseconds

    @classmethod
    def get_by_token(cls, account, token):
        account_key = Account.resolve_key(account)
        return cls.query(cls.sender == account_key, cls.token == token).get()

    def get_reaction(self, account_key):
        self._upgrade_reactions()
        try:
            index = self.reaction_keys.index(account_key)
            return self.reaction_types[index]
        except ValueError:
            return None

    @property
    def is_expired(self):
        return self.start < datetime.utcnow() - config.CHUNK_MAX_AGE

    @property
    def localized_start(self):
        if not self.timezone:
            return None
        tz = pytz.timezone(self.timezone)
        return pytz.utc.localize(self.start).astimezone(tz)

    @property
    def lowfi_audio_url(self):
        return self.url.rsplit('.', 1)[0] + '-l.mp3'

    def public_for_chunk_id(self, chunk_id, version):
        data = {
            'duration': self.duration,
            'end': self.end,
            'id': chunk_id,
            'sender_id': self.sender.id(),
            'start': self.start,
        }
        if version < 2:
            del data['sender_id']
            data['sender'] = self.sender.get()
        if version >= 26:
            elements = []
            prevs, items = tee(self.text_segments, 2)
            for p, s in izip(chain([None], prevs), items):
                elements.extend([s.start - (p.start + p.duration if p else 0), s.duration, s.text])
            data['text'] = elements
        elif version >= 24:
            data['text'] = self.text_segments
        elif version >= 19:
            data['text'] = self.text
        if version >= 36:
            data['reactions'] = {str(k.id()): v for k, v in self.reactions.iteritems()}
        elif version >= 25:
            data['reactions'] = [k.id() for k in self.reactions]
        if version >= 29:
            data['url'] = self.url
        else:
            data['audio_url'] = self.legacy_url
        if version >= 32:
            data['external_plays'] = self.external_plays
        if version >= 33:
            data['external_content_id'] = self.external_content_id
        if version >= 35:
            data['attachments'] = self.attachments
        return data

    def public(self, version=None, **kwargs):
        return self.public_for_chunk_id(self.key.id(), version)

    @property
    def reactions(self):
        self._upgrade_reactions()
        return dict(zip(self.reaction_keys, self.reaction_types))

    def set_reaction(self, account_key, reaction_type):
        self._upgrade_reactions()
        try:
            index = self.reaction_keys.index(account_key)
            if reaction_type is None:
                del self.reaction_keys[index]
                del self.reaction_types[index]
                return
            self.reaction_types[index] = reaction_type
        except ValueError:
            if reaction_type is None:
                return
            self.reaction_keys.append(account_key)
            self.reaction_types.append(reaction_type)

    @property
    def text(self):
        return ' '.join(s.text for s in self.text_segments)

    @property
    def url(self):
        return files.storage_url(self.payload)

    @property
    def legacy_url(self):
        return files.legacy_url(self.payload)

    def _upgrade_reactions(self):
        if len(self.reaction_keys) != len(self.reaction_types):
            self.reaction_types = [u'👍'] * len(self.reaction_keys)


class ChunkInStream(Chunk):
    # Points to the real chunk.
    chunk_id = ndb.IntegerProperty()

    @classmethod
    def from_chunk(cls, chunk):
        props = dict((p._code_name, p.__get__(chunk, Chunk))
                     for p in Chunk._properties.itervalues()
                     if type(p) is not ndb.ComputedProperty)
        return cls(chunk_id=chunk.key.id(), **props)

    def public(self, version=None, **kwargs):
        return self.public_for_chunk_id(self.chunk_id, version)


class Config(ndb.Model):
    value = ndb.JsonProperty()


class YouTubeIdProperty(ndb.StringProperty):
    def _from_base_type(self, value):
        # This is a patch to support migration from non-repeated values that were None.
        if value is None:
            logging.debug('Had to convert None to u\'\' in _from_base_type')
            return u''
        return super(YouTubeIdProperty, self)._from_base_type(value)

    def _to_base_type(self, value):
        if value is None:
            logging.debug('Had to convert None to u\'\' in _to_base_type')
            return u''
        return super(YouTubeIdProperty, self)._to_base_type(value)

    def _validate(self, value):
        if value is None:
            logging.debug('Had to ignore None in _validate')
            return
        return super(YouTubeIdProperty, self)._validate(value)


class Content(ndb.Model):
    CDN_BASE_URL = 'https://d32au24mly9y2n.cloudfront.net'
    UPLOAD_URL_ROOTS = [
        'https://d32au24mly9y2n.cloudfront.net/hls/',
        'https://d32au24mly9y2n.cloudfront.net/',
        'https://s.reaction.cam/hls/',
        'https://s.reaction.cam/',
        'https://storage.googleapis.com/rcam/',
    ]

    comment_count = ndb.IntegerProperty(default=0)
    created = ndb.DateTimeProperty()
    creator = ndb.KeyProperty(Account, required=True)
    creator_twitter = ndb.StringProperty(indexed=False)
    dedupe = ndb.StringProperty()
    duration = ndb.IntegerProperty(default=0, indexed=False)
    first_related_creator = ndb.KeyProperty(Account)
    metadata = ndb.JsonProperty()
    original_url = ndb.StringProperty()
    properties = ndb.JsonProperty()
    related_count = ndb.IntegerProperty(default=0)
    related_to = ndb.KeyProperty(kind='Content')
    request = ndb.KeyProperty(kind='ContentRequestPublic')
    slug = ndb.StringProperty()
    sort_bonus = ndb.IntegerProperty(default=0)
    sort_bonus_penalty = ndb.IntegerProperty(default=0, indexed=False)
    sort_index = ndb.IntegerProperty()
    tags = ndb.StringProperty(repeated=True)
    tags_history = ndb.StringProperty(indexed=False, repeated=True)
    thumb_url_ = ndb.StringProperty('thumb_url', indexed=False)
    title = ndb.StringProperty(indexed=False)
    useragent = ndb.StringProperty(indexed=False)
    video_url_ = ndb.StringProperty('video_url', indexed=False)
    views = ndb.IntegerProperty(default=0)
    views_real = ndb.IntegerProperty(default=0)
    votes = ndb.IntegerProperty(default=0)
    votes_real = ndb.IntegerProperty(default=0)
    youtube_broken = ndb.BooleanProperty(default=False, indexed=False)
    youtube_id_history = YouTubeIdProperty('youtube_id', repeated=True)
    youtube_reaction_views = ndb.IntegerProperty()
    youtube_reaction_views_updated = ndb.DateTimeProperty(indexed=False)
    youtube_views = ndb.IntegerProperty()
    youtube_views_updated = ndb.DateTimeProperty(indexed=False)

    def add_comment_count(self, account, count=1):
        if count < 0 and -count > self.comment_count:
            count = -self.comment_count
        self.comment_count += count
        if account.key != self.creator:
            if account.quality >= 4:
                bonus = 2000
            elif account.quality == 3:
                bonus = 1500
            elif account.quality == 2:
                bonus = 750 + min(account.follower_count * 5, 250)
            elif account.quality == 1:
                bonus = 250 + min(account.follower_count * 5, 500)
            else:
                bonus = 100
            self.add_sort_index_bonus(bonus * count)

    def add_related_count(self, account, count=1, account_reacted_already=False):
        # Note: Returns True if this resulted in first_related_creator being set.
        first = False
        if count < 0 and -count > self.related_count:
            count = -self.related_count
        self.related_count += count
        if self.related_count > 0:
            self.add_tag('is reacted', allow_restricted=True)
        if account.key == self.creator:
            return False
        if account.key == self.first_related_creator:
            account_reacted_already = True
        if count > 0 and not self.first_related_creator:
            if self.related_count < 3:
                # Bump the content sort index as if it was just created.
                i = self.get_sort_index() + self.sort_bonus - self.sort_bonus_penalty
                self.sort_index = max(i, self.sort_index)
            self.first_related_creator = account.key
            first = True
        if account.quality > 1:
            bonus = 13000 + account.quality * (1000 + min(account.follower_count * 100, 2000))
        elif account.quality == 1:
            bonus = 2500 + min(account.follower_count * 200, 5000)
        else:
            bonus = 500
        if account_reacted_already:
            # Don't let one user make content trend.
            bonus //= 100
        self.add_sort_index_bonus(bonus * count)
        if 'is hot' in self.tags and self.related_count >= (6 if self.related_to else 3):
            # Allow reactions to become originals with enough reactions.
            self.add_tag('original', allow_restricted=True)
        return first

    def add_sort_index_bonus(self, bonus):
        if bonus < 0 and -bonus > self.sort_bonus:
            bonus = -self.sort_bonus
        age = (datetime.utcnow() - self.created).total_seconds()
        val = float(age + self.sort_bonus) ** 2 / 186624000000
        bonus_w_penalty = int(bonus * min(max(1 - val, 0.1), 1))
        self.sort_bonus += bonus
        self.sort_bonus_penalty += bonus - bonus_w_penalty
        self.sort_index += bonus_w_penalty
        if self.sort_bonus > 50000:
            self.add_tag('is hot', allow_restricted=True)

    def add_tag(self, tag, **kwargs):
        self.add_tags([tag], **kwargs)

    def add_tags(self, tags, allow_restricted=False, **kwargs):
        if isinstance(tags, basestring):
            tags = self.parse_tags(tags, allow_restricted=allow_restricted)
        else:
            tags = set(tt for t in tags for tt in self.parse_tags(t, allow_restricted=allow_restricted))
        self.set_tags(set(self.tags) | tags, allow_restricted=allow_restricted, **kwargs)

    def add_view_count(self, account, is_bot=False, count=1):
        # Note: `account` may be None since anonymous users can watch videos.
        if not account or account.key != self.creator:
            if is_bot:
                bonus = 1
            else:
                bonus = 5
            self.add_sort_index_bonus(bonus * count)
        self.views += count
        if not is_bot:
            self.views_real += count

    def add_vote_count(self, account, is_bot=False, count=1):
        if account.key != self.creator:
            if account.quality >= 4:
                bonus = 5000
            elif account.quality == 3:
                bonus = 4000
            elif account.quality == 2:
                bonus = 2000 + min(account.follower_count * 5, 1000)
            elif account.quality == 1:
                bonus = 1000 + min(account.follower_count * 5, 1000)
            else:
                bonus = 500
            if is_bot:
                bonus //= 10
            self.add_sort_index_bonus(bonus * count)
        self.votes += count
        if self.views < self.votes:
            self.views = self.votes
        if not is_bot:
            self.votes_real += count

    def became_public(self, creator, related_to=None, first_time=False):
        # Public tags logic.
        if creator.publish:
            # Add the published tag for whitelisted accounts.
            self.add_tag('published', allow_restricted=True)
        # Initial content bonus.
        if creator.quality >= 4:
            self.add_sort_index_bonus(16000)
        elif creator.quality == 3:
            self.add_sort_index_bonus(15000)
        elif creator.quality == 2:
            self.add_sort_index_bonus(10000)
        elif creator.quality == 1:
            self.add_sort_index_bonus(1000)
        # Remaining logic is only for reactions.
        if not related_to:
            return
        # Transfer tags from original to content.
        for tag in related_to.tags:
            if ' ' in tag or tag in config.NON_TRANSFERABLE_TAGS:
                continue
            self.add_tag(tag)

    @classmethod
    def clean_title(cls, title):
        suffixes = [u' - YouTube']
        for s in suffixes:
            if not title.endswith(s):
                continue
            title = title[:-len(s)]
        return title.strip()

    @classmethod
    def decorate(cls, content_list, include_creator=False, include_related=False, for_account_key=None):
        keys = []
        if include_creator:
            keys.extend({c.creator for c in content_list})
        if include_related:
            keys.extend({c.related_to for c in content_list if c.related_to})
        if for_account_key:
            # Also look up votes for every piece of content.
            keys.extend(ndb.Key('ContentVote', c.key.id(), parent=for_account_key)
                        for c in content_list)
        if not keys:
            return {}, [None] * len(content_list)
        entities = ndb.get_multi(keys)
        if for_account_key:
            # Remove vote data from the lookup.
            votes = entities[-len(content_list):]
            entities = entities[:-len(content_list)]
        else:
            votes = [None] * len(content_list)
        lookup = {e.key: e for e in entities if e}
        return lookup, votes

    def decoration_info(self, include_creator=False, include_related=False, for_account_key=None):
        keys = []
        if include_creator:
            keys.append(self.creator)
        if include_related and self.related_to:
            keys.append(self.related_to)
        if for_account_key:
            keys.append(ndb.Key('ContentVote', self.key.id(), parent=for_account_key))
        if not keys:
            return (None, None, None)
        entities = ndb.get_multi(keys)
        return (
            entities[0] if include_creator else None,
            entities[1 if include_creator else 0] if include_related and self.related_to else None,
            entities[-1] is not None if for_account_key else False)

    @classmethod
    def get_by_youtube_id_async(cls, youtube_id):
        return cls.query(cls.youtube_id_history == youtube_id).get_async()

    @classmethod
    def get_sort_index(cls):
        delta = datetime.utcnow() - datetime(2017, 5, 1)
        return int(delta.total_seconds())

    @property
    def has_been_public(self):
        return any(not self.is_tag_unlisted(t) for t in self.tags_history)

    @property
    def is_public(self):
        return any(not self.is_tag_unlisted(t) for t in self.tags)

    @classmethod
    def is_tag_restricted(cls, tag):
        tag = cls.parse_tag(tag, allow_restricted=True)
        if not tag:
            return False
        return ' ' in tag or tag in config.RESTRICTED_TAGS

    @classmethod
    def is_tag_unlisted(cls, tag):
        tag = cls.parse_tag(tag, allow_restricted=True)
        if not tag:
            return False
        return ' ' in tag or tag in config.UNLISTED_TAGS

    @classmethod
    def make_slug(cls, value):
        if not isinstance(value, basestring):
            raise TypeError('Expected string')
        value = re.sub(r'[^a-z0-9]+', ' ', value.lower())
        value = re.sub(r'\s+', '-', value.strip())
        if len(value) > 50:
            value = value[:46]
            if not value.endswith('-'):
                value += '-'
            value += random.base62(3)
        return value

    @classmethod
    def new(cls, allow_restricted_tags=False, tags=[], **kwargs):
        if 'created' not in kwargs:
            kwargs['created'] = datetime.utcnow()
        if 'sort_index' not in kwargs:
            kwargs['sort_index'] = cls.get_sort_index()
        content = cls()
        for name, value in kwargs.iteritems():
            setattr(content, name, value)
        content.set_tags(tags, allow_restricted=allow_restricted_tags)
        if 'slug' not in kwargs and content.is_public:
            # Auto-set slug if content is public.
            content.slug = content.slug_from_video_url()
        return content

    @classmethod
    def parse_tag(cls, tag_string, allow_restricted=False):
        tags = cls.parse_tags(tag_string, allow_restricted=allow_restricted)
        if len(tags) != 1:
            return None
        return iter(tags).next()

    @classmethod
    def parse_tags(cls, tags_string, allow_restricted=False, separator=','):
        tag_strings = [t.strip(' #').lower() for t in tags_string.split(separator)]
        return {t for t in tag_strings if t and (allow_restricted or not cls.is_tag_restricted(t))}

    def public(self, version=None, **kwargs):
        data = {
            'id': self.key.id(),
            'created': self.created,
            'duration': self.duration,
            'original_url': self.original_url,
            'tags': self.visible_tags,
            'thumb_url': self.thumb_url,
            'title': self.title,
            'url': self.web_url,
            'video_url': self.video_url,
            'votes': self.votes,
        }
        if version >= 39:
            data['views'] = self.views
        if version < 43:
            if not data['video_url']:
                data['video_url'] = 'https://www.reaction.cam/s/broken.mp4'
        if version >= 43:
            data['creator_id'] = self.creator.id()
            data['related_count'] = self.related_count
        if version >= 46:
            data['comment_count'] = self.comment_count
        if version >= 53:
            data['properties'] = self.properties or {}
        if version >= 54:
            data['request_id'] = self.request.id() if self.request else None
        if version >= 56:
            data['youtube_url'] = self.youtube_url
        return data

    def remove_tag(self, tag, **kwargs):
        self.remove_tags([tag], **kwargs)

    def remove_tags(self, tags, allow_restricted=False):
        if isinstance(tags, basestring):
            tags = self.parse_tags(tags)
        else:
            tags = set(tags)
        if not allow_restricted:
            tags = set(t for t in tags if not self.is_tag_restricted(t))
        self.set_tags(set(self.tags) - tags, allow_restricted=True, keep_restricted=False)

    @property
    def s3_key(self):
        if not self.video_url:
            return None
        valid_prefixes = [
            'https://d32au24mly9y2n.cloudfront.net/',
        ]
        if not any(self.video_url.startswith(p) for p in valid_prefixes):
            return None
        return self.video_url.split('/')[-1]

    @property
    def search_rank(self):
        # Note: This value must never be negative.
        return 17000000 + self.sort_index // 1800 + self.sort_bonus

    def set_tags(self, tags, allow_restricted=False, keep_restricted=True):
        if isinstance(tags, basestring):
            tags = self.parse_tags(tags, allow_restricted=allow_restricted)
        else:
            tags = set(t for t in tags if allow_restricted or not self.is_tag_restricted(t))
        for tag in tags:
            if tag not in self.tags_history:
                self.tags_history.append(tag)
        if keep_restricted:
            # Keep all restricted tags regardless of whether they're in the new tags list.
            restricted = set(filter(lambda t: self.is_tag_restricted(t), self.tags))
            self.tags = list(tags | restricted)
        else:
            self.tags = list(tags)
        if not self.tags:
            raise errors.InvalidArgument('Content must have at least one valid tag')
        if self.is_public and not self.slug:
            # A public tag was added to user content.
            self.slug = self.slug_from_video_url()

    def set_youtube_id(self, value):
        if value is None:
            # Represent no value in the history list as the empty string.
            value = ''
        if self.youtube_id_history and self.youtube_id_history[-1] == value:
            # Don't do anything if this is already the most recent id.
            return
        if not self.youtube_id_history and value == '':
            # Also consider setting an empty value with no history a no-op.
            return
        # Add the new entry to history, moving it to the end if it's already there.
        try:
            self.youtube_id_history.remove(value)
        except ValueError:
            pass
        self.youtube_id_history.append(value)
        # Also reset all metadata about the YouTube video.
        self.youtube_broken = False
        self.youtube_views = None
        self.youtube_views_updated = None

    def set_youtube_views(self, count):
        delta = count - (self.youtube_views or 0)
        if delta < 0:
            logging.warning('YouTube views for %d reduced from %d to %d',
                            self.key.id(), self.youtube_views, count)
            return 0
        self.youtube_views = count
        self.youtube_views_updated = datetime.utcnow()
        if delta > 0:
            self.add_sort_index_bonus(delta * 15)
        return delta

    def slug_from_video_url(self):
        if not self.video_url:
            return None
        for root in self.UPLOAD_URL_ROOTS:
            if not self.video_url.startswith(root):
                continue
            filename = self.video_url[len(root):]
            dot_index = filename.find('.')
            slug = filename[:dot_index] if dot_index > -1 else filename
            if '/' in slug:
                continue
            return slug
        return None

    @property
    def sort_base(self):
        return self.sort_index - self.sort_bonus + self.sort_bonus_penalty

    def thumb_url_from_video_url(self):
        video_url = self.video_url
        if not video_url:
            return None
        for root in self.UPLOAD_URL_ROOTS:
            if not video_url.startswith(root):
                continue
            filename = video_url[len(root):]
            dot_index = filename.find('.')
            bare_filename = filename[:dot_index] if dot_index > -1 else (filename + '_')
            if '/' in bare_filename:
                continue
            # TODO: Fix this terrible hack!
            if 'd32au24mly9y2n' in video_url:
                service = 's3'
            elif 'd266pkvbz16kse' in video_url:
                service = 's3b'
            else:
                logging.warning('Cannot create thumbnail URL for %r', video_url)
                return None
            return '%s/%s/%s.jpg' % (self.CDN_BASE_URL, service, bare_filename)
        return None

    @property
    def thumb_url(self):
        if not self.thumb_url_:
            return None
        return files.storage_url(self.thumb_url_)

    @thumb_url.setter
    def thumb_url(self, value):
        self.thumb_url_ = files.storage_url(value)

    @classmethod
    def valid_upload_url(cls, url):
        if not isinstance(url, basestring):
            return False
        return any(url.startswith(r) for r in cls.UPLOAD_URL_ROOTS)

    @property
    def video_url(self):
        if not self.video_url_:
            return None
        return files.storage_url(self.video_url_)

    @video_url.setter
    def video_url(self, value):
        self.video_url_ = files.storage_url(value)

    def visible_by(self, account):
        if 'deleted' in self.tags:
            return False
        if self.is_public:
            return True
        if not account:
            return False
        return self.creator == Account.resolve_key(account)

    @property
    def visible_tags(self):
        tags = [t for t in self.tags if t not in config.INTERNAL_TAGS]
        if 'featured' in self.tags_history and 'featured' not in self.tags:
            tags.append('exfeatured')
        tags.sort()
        return tags

    @property
    def web_url(self):
        if not self.slug:
            return None
        return 'https://www.reaction.cam/v/%s' % (self.slug,)

    @property
    def youtube_id(self):
        if not self.youtube_id_history:
            return None
        return self.youtube_id_history[-1] or None

    @property
    def youtube_url(self):
        vid = self.youtube_id
        if not vid:
            return None
        return 'https://www.youtube.com/watch?v=' + vid

    def _pre_put_hook(self):
        if self.youtube_id_history:
            # Clean up redundant data in history.
            if not any(self.youtube_id_history):
                self.youtube_id_history = []
            elif self.youtube_id_history[-1] != '':
                self.youtube_id_history = filter(None, self.youtube_id_history)

    def _set_attributes(self, attrs):
        if 'thumb_url' in attrs:
            attrs['thumb_url_'] = files.storage_url(attrs.pop('thumb_url'))
        if 'video_url' in attrs:
            attrs['video_url_'] = files.storage_url(attrs.pop('video_url'))
        super(Content, self)._set_attributes(attrs)


class ContentComment(ndb.Model):
    created = ndb.DateTimeProperty(auto_now_add=True)
    creator = ndb.KeyProperty(Account, required=True)
    offset = ndb.IntegerProperty()
    reply_to = ndb.KeyProperty(kind='ContentComment')
    text = ndb.StringProperty(indexed=False)

    def public(self, creator=None, version=None, **kwargs):
        data = {
            'created': self.created,
            'creator_id': self.creator.id(),
            'id': self.key.id(),
            'offset': self.offset,
            'text': self.text,
        }
        if creator and creator.key == self.creator:
            data['creator_image_url'] = creator.image_url
            data['creator_username'] = creator.username
        if version >= 47:
            data['reply_to'] = self.reply_to.id() if self.reply_to else None
        return data


class ContentRequest(ndb.Model):
    content = ndb.KeyProperty(Content)
    requested = ndb.DateTimeProperty(auto_now_add=True)
    requested_by = ndb.KeyProperty(Account)


class ContentRequestPublic(ndb.Model):
    closed = ndb.BooleanProperty(indexed=False)
    content = ndb.KeyProperty(Content, required=True)
    properties = ndb.JsonProperty()
    requested = ndb.DateTimeProperty(auto_now_add=True)
    requested_by = ndb.KeyProperty(Account, required=True)
    sort_index = ndb.IntegerProperty()
    tags = ndb.StringProperty(repeated=True)
    wallet = ndb.KeyProperty(indexed=False, kind='Wallet')

    SPECIAL_STATES = {'approved', 'archived', 'denied', 'pending'}

    def public(self, version=None, **kwargs):
        data = {
            'closed': self.closed or False,
            'id': self.key.id(),
            'properties': self.properties or {},
        }
        if 'reward' in kwargs:
            data['reward'] = kwargs.pop('reward')
        return data

    def set_state(self, new_state):
        if not isinstance(new_state, basestring):
            raise TypeError('State must be a string')
        if new_state not in self.SPECIAL_STATES:
            raise ValueError('%r is not a valid state' % (new_state,))
        self.tags = sorted(set(self.tags) - self.SPECIAL_STATES | {new_state})

    @property
    def wallet_owner(self):
        # TODO: Consider supporting this to be different from requested_by.
        return self.requested_by


class ContentRequestPublicEntry(ndb.Model):
    VALID_STATUSES = {
        'active',
        'closed',
        'denied',
        'inactive',
        'open',
        'pending-review',
        'pending-upload',
        'pending-youtube',
    }

    account = ndb.KeyProperty(Account, required=True)
    content = ndb.KeyProperty(Content)
    created = ndb.DateTimeProperty(auto_now_add=True)
    request = ndb.KeyProperty(ContentRequestPublic, required=True)
    reward_earned = ndb.IntegerProperty()
    status = ndb.StringProperty(default='pending-upload', required=True)
    status_reason = ndb.StringProperty(indexed=False)
    updated = ndb.DateTimeProperty(auto_now=True, indexed=False)
    youtube_id = ndb.StringProperty(indexed=False)
    youtube_views = ndb.IntegerProperty(indexed=False)

    def public(self, version=None, **kwargs):
        if self.youtube_id:
            url = 'https://www.youtube.com/watch?v=%s' % (self.youtube_id,)
        else:
            url = None
        return {
            'created': self.created,
            'reward_earned': self.reward_earned or 0,
            'youtube_url': url,
            'youtube_views': self.youtube_views or 0,
        }

    @classmethod
    def resolve_key(cls, request_key, account_key):
        if not isinstance(request_key, ndb.Key):
            raise TypeError('request_key must be a Key')
        if request_key.kind() != 'ContentRequestPublic':
            raise ValueError('request_key must be a ContentRequestPublic key')
        if not isinstance(account_key, ndb.Key):
            raise TypeError('account_key must be a Key')
        if account_key.kind() != 'Account':
            raise ValueError('account_key must be an Account key')
        entry_id = '%d.%d' % (request_key.id(), account_key.id())
        return ndb.Key(cls, entry_id)

    @classmethod
    def restore(cls, *args, **kwargs):
        return cls.restore_async(*args, **kwargs).get_result()

    @classmethod
    @ndb.transactional_tasklet
    def restore_async(cls, entry_key):
        if isinstance(entry_key, tuple):
            request_key, account_key = entry_key
            entry_key = cls.resolve_key(request_key, account_key)
        entry = yield entry_key.get_async()
        if not entry:
            raise errors.ResourceNotFound('That entry does not exist')
        if entry.status not in ('active', 'denied', 'inactive'):
            raise errors.ForbiddenAction('The entry cannot be restored in its current state')
        entry.status = 'pending-review'
        entry.status_reason = None
        yield entry.put_async()
        raise ndb.Return(entry)

    @classmethod
    def review(cls, *args, **kwargs):
        return cls.review_async(*args, **kwargs).get_result()

    @classmethod
    @ndb.transactional_tasklet(xg=True)
    def review_async(cls, entry_key, approved, reason=None):
        if isinstance(entry_key, tuple):
            request_key, account_key = entry_key
            entry_key = cls.resolve_key(request_key, account_key)
        entry = yield entry_key.get_async()
        if not entry:
            raise errors.ResourceNotFound('That entry does not exist')
        if entry.status != 'pending-review':
            raise errors.ForbiddenAction('The entry cannot be reviewed in its current state')
        entry.status = 'active' if approved else 'denied'
        entry.status_reason = reason
        content, _ = yield entry.content.get_async(), entry.put_async()
        if approved and 'is approved' not in content.tags:
            content.add_tag('is approved', allow_restricted=True)
            yield content.put_async()
        raise ndb.Return(entry)

    @classmethod
    def reward(cls, *args, **kwargs):
        return cls.reward_async(*args, **kwargs).get_result()

    @classmethod
    @ndb.transactional_tasklet(xg=True)
    def reward_async(cls, entry_key, content, wallet_owner_key, wallet_key):
        if not isinstance(entry_key, ndb.Key) or entry_key.kind() != cls._get_kind():
            raise TypeError('Invalid entry_key value')
        if not isinstance(content, Content):
            raise TypeError('Invalid content value')
        if not isinstance(wallet_owner_key, ndb.Key) or wallet_owner_key.kind() != 'Account':
            raise TypeError('Invalid wallet_owner_key value')
        if not isinstance(wallet_key, ndb.Key) or wallet_key.kind() != 'Wallet':
            raise TypeError('Invalid wallet_key value')
        entry = yield entry_key.get_async()
        if not entry:
            raise ValueError('Entry does not exist')
        # Only update active entries.
        if entry.status != 'active':
            logging.debug('Entry %s is not active (content %d)', entry.key.id(), content.key.id())
            raise ndb.Return(0)
        # If the YouTube video is broken state switches to inactive.
        if content.youtube_broken:
            entry.status = 'inactive'
            entry.status_reason = 'We could not load your YouTube video. It may have been blocked or deleted.'
            yield entry.put_async()
            logging.debug('Entry %s became inactive due to broken YouTube video (content %d)',
                entry.key.id(), content.key.id())
            raise ndb.Return(0)
        # TODO: Validate YouTube description.
        old_youtube_views = entry.youtube_views or 0
        new_youtube_views = content.youtube_views or 0
        # TODO: Consider a threshold to avoid updating too often.
        if new_youtube_views <= old_youtube_views:
            logging.debug('Entry %s had no additional YouTube views to reward (content %d)',
                entry.key.id(), content.key.id())
            raise ndb.Return(0)
        # Determine reward amount, up to a maximum every time (to distribute reward better).
        amount = min(new_youtube_views - old_youtube_views, 250)
        # TODO: Support rare corner cases where user may not have a wallet.
        target_wallet_id = 'account_%d' % (entry.account.id(),)
        target_wallet_key = Wallet.key_from_id(target_wallet_id)
        tx = yield Wallet.create_tx_async(
            wallet_owner_key, wallet_key,
            target_wallet_key, amount, u'Request reward',
            require_full_amount=False)
        try:
            _, _, _, tx2 = yield tx()
            actual_amount = tx2.delta
        except WalletInsufficientFunds:
            actual_amount = 0
        assert 0 <= actual_amount <= amount
        entry.reward_earned = (entry.reward_earned or 0) + actual_amount
        if not actual_amount and entry.youtube_views == new_youtube_views:
            # Nothing changed.
            raise ndb.Return(0)
        # Increment YouTube views on entry only by the awarded amount so we don't forget unpaid rewards.
        entry.youtube_views = old_youtube_views + actual_amount
        yield entry.put_async()
        logging.debug('Added %d to wallet for owner of entry %s (content %d)',
            actual_amount, entry.key.id(), content.key.id())
        if actual_amount < amount:
            logging.warning('Could not award remaining %d due to insufficient funds',
                amount - actual_amount)
        raise ndb.Return(actual_amount)

    @classmethod
    def split_key(cls, entry_key):
        request_id, account_id = entry_key.id().split('.')
        return int(request_id), int(account_id)

    @classmethod
    def update(cls, *args, **kwargs):
        return cls.update_async(*args, **kwargs).get_result()

    @classmethod
    @ndb.transactional_tasklet
    def update_async(cls, entry_key, content, reset=False):
        if isinstance(entry_key, tuple):
            request_key, account_key = entry_key
            entry_key = cls.resolve_key(request_key, account_key)
        else:
            request_key, account_key = cls.split_key(entry_key)
        entry = yield entry_key.get_async()
        if entry:
            needs_put = entry._set_content(content, reset)
        else:
            entry = cls(key=entry_key, account=account_key, request=request_key)
            entry._set_content(content, reset)
            needs_put = True
        if needs_put:
            yield entry.put_async()
        raise ndb.Return((entry, needs_put))

    def _pre_put_hook(self):
        if self.status not in self.VALID_STATUSES:
            raise ValueError('%r is not a valid entry status' % (self.status,))

    def _set_content(self, content, reset):
        if content and reset:
            raise errors.InvalidArgument('Cannot both specify content and reset at the same time')
        if reset:
            if self.status not in ('active', 'denied', 'inactive', 'pending-review', 'pending-upload', 'pending-youtube'):
                raise errors.ForbiddenAction('Cannot reset entry in status %r' % (self.status,))
            logging.debug(
                'Resetting request %d entry from %r for account %d '
                '(content: %s, youtube_id: %s, youtube_views: %s)',
                self.request.id(), self.status, self.account.id(),
                self.content.id() if self.content else None,
                self.youtube_id, self.youtube_views)
            self.content = None
            self.status = 'open'
            self.status_reason = None
            self.youtube_id = None
            self.youtube_views = None
            return True
        if not content:
            if self.status == 'open':
                self.status = 'pending-upload'
                return True
            return False
        if content.creator != self.account:
            raise errors.ForbiddenAction('That content was created by another account')
        did_change = False
        if self.content and self.content != content.key:
            raise errors.ForbiddenAction('Cannot change content on entry')
        if self.status in ('open', 'pending-upload'):
            assert self.content is None
            assert self.youtube_id is None
            self.content = content.key
            # Note: Status may change again in next if statement!
            self.status = 'pending-youtube'
            did_change = True
        if self.status == 'pending-youtube':
            assert self.content
            assert self.youtube_id is None
            if content.youtube_id:
                self.youtube_id = content.youtube_id
                self.youtube_views = content.youtube_views
                self.status = 'pending-review'
                did_change = True
        return did_change


class ContentVote(ndb.Model):
    content = ndb.KeyProperty(Content)
    voted = ndb.DateTimeProperty(auto_now_add=True)


class CounterShard(ndb.Model):
    count = ndb.IntegerProperty(default=0, indexed=False)

    @classmethod
    def decrement_async(cls, name, delta=1, num_shards=3, parent=None):
        return cls._change_async(name, -delta, num_shards, parent)

    @classmethod
    @ndb.tasklet
    def get_count_async(cls, name, num_shards=3, parent=None):
        context = ndb.get_context()
        prefix = parent.urlsafe() if parent else ''
        total = yield context.memcache_get(prefix + name)
        if total is not None:
            raise ndb.Return(total)
        all_keys = cls._make_keys(name, num_shards, parent=parent)
        counters = yield ndb.get_multi_async(all_keys)
        total = 0
        for counter in counters:
            if counter is not None:
                total += counter.count
        yield context.memcache_add(prefix + name, total, 60)
        raise ndb.Return(total)

    @classmethod
    def increment_async(cls, name, delta=1, num_shards=3, parent=None):
        return cls._change_async(name, delta, num_shards, parent)

    @classmethod
    @ndb.transactional_tasklet
    def _change_async(cls, name, delta, num_shards, parent):
        if delta == 0:
            return
        index = random.randint(0, num_shards - 1)
        key = cls._key(name, index, parent)
        counter = yield key.get_async()
        if not counter:
            counter = cls(key=key)
        counter.count += delta
        prefix = parent.urlsafe() if parent else ''
        context = ndb.get_context()
        if delta > 0:
            memcache_future = context.memcache_incr(prefix + name, delta=delta)
        else:
            memcache_future = context.memcache_decr(prefix + name, delta=-delta)
        yield counter.put_async(), memcache_future

    @classmethod
    def _key(cls, name, index, parent=None):
        return ndb.Key(cls, '{}-{:d}'.format(name, index), parent=parent)

    @classmethod
    def _make_keys(cls, name, num_shards, parent=None):
        return [cls._key(name, i, parent) for i in xrange(num_shards)]


class Device(ndb.Model):
    api_version = ndb.IntegerProperty(default=1, indexed=False)
    app = ndb.StringProperty(default='im.rgr.RogerApp.voip')
    created = ndb.DateTimeProperty(auto_now_add=True, indexed=False)
    device_id = ndb.StringProperty(indexed=False)
    device_info = ndb.StringProperty(indexed=False)
    environment = ndb.StringProperty(indexed=False)
    failures = ndb.IntegerProperty(default=0, indexed=False)
    last_success = ndb.DateTimeProperty(auto_now_add=True, indexed=False)
    platform = ndb.StringProperty(indexed=False)
    token = ndb.StringProperty()
    total_failures = ndb.IntegerProperty(default=0, indexed=False)
    total_successes = ndb.IntegerProperty(default=0, indexed=False)
    updated = ndb.DateTimeProperty(auto_now=True, indexed=False)

    @classmethod
    def add(cls, account, api_version, app, device_id, device_info, platform, token, **kwargs):
        # TODO: Use user agent to determine app and platform.
        if platform not in config.VALID_NOTIF_PLATFORMS:
            raise errors.InvalidArgument('Unsupported platform %s' % (platform,))
        if not device_id:
            # Consider empty strings as None.
            device_id = None
        account_key = Account.resolve_key(account)
        # We'll look for an existing device token and any old ones to delete.
        entity = None
        keys_to_delete = []
        # Go through every existing device token.
        devices = cls.query(ancestor=account_key).fetch()
        for dvc in devices:
            if (dvc.app, dvc.platform, dvc.device_id) != (app, platform, device_id):
                # Ignore tokens for other apps, platforms and devices.
                continue
            if dvc.token != token:
                # Delete all previous tokens for this platform and device.
                keys_to_delete.append(dvc.key)
                continue
            # This token was already assigned to the current user.
            new_device_info = device_info and device_info != dvc.device_info
            if dvc.api_version != api_version or new_device_info or dvc.failures > 0:
                # Reset failures of the device.
                dvc.failures = 0
                # Update the desired API version of the device if it has changed.
                dvc.api_version = api_version
                dvc.device_info = device_info
                dvc.populate(**kwargs)
                dvc.put()
                logging.debug('Updated existing device token info')
            else:
                logging.debug('Device token already exists and up-to-date')
            entity = dvc
        if not entity:
            # Check if the token is in use by another user (in which case we will delete it).
            device_key = cls.query(cls.token == token).get(keys_only=True)
            if device_key and device_key.parent() != account_key:
                logging.debug('Claiming existing token in use by account %d',
                              device_key.parent().id())
                keys_to_delete.append(device_key)
                AccountEvent.create(account_key, 'Device Claimed',
                                    event_class='warning',
                                    properties={'OtherAccount': device_key.parent().id()})
            else:
                logging.debug('Created new device token')
            # Create a new device entity for the registered token.
            entity = cls(id=token, parent=account_key, api_version=api_version, app=app,
                         device_id=device_id, device_info=device_info, platform=platform,
                         token=token, **kwargs)
            entity.put()
            AccountEvent.create(account_key, 'Added Device',
                                event_class='debug',
                                properties={'UserAgent': device_info})
        # Don't allow more than a certain number of tokens at a time.
        if len(devices) > config.MAX_DEVICE_TOKENS:
            devices.sort(key=lambda d: d.updated, reverse=True)
            keys_to_delete.extend(d.key for d in devices[config.MAX_DEVICE_TOKENS:])
        # Delete devices.
        if keys_to_delete:
            formatted_keys = '\n'.join('- {}'.format(k.id()) for k in keys_to_delete)
            logging.debug('Deleting the following tokens:\n%s', formatted_keys)
            ndb.delete_multi(keys_to_delete)
        return entity

    @classmethod
    def delete(cls, account, token):
        account_key = Account.resolve_key(account)
        ndb.Key(cls, token, parent=account_key).delete()

    def public(self, version=None, **kwargs):
        data = {
            'api_version': self.api_version,
            'platform': self.platform,
            'token': self.token,
        }
        if version >= 40:
            data['created'] = self.created
            data['total_failures'] = self.total_failures
            data['total_successes'] = self.total_successes
            match = re.match(r'(\w+)/\d+', self.device_info or '')
            data['minimum_build'] = config.MINIMUM_BUILD.get(match.group(1))
        return data

    def update_statistics(self, success=None, **kwargs):
        if not isinstance(success, bool):
            raise TypeError('Success needs to be True/False')
        if success:
            if self.failures:
                logging.info('Restoring endpoint from %d failure(s) to 0', self.failures)
            self.failures = 0
            self.last_success = datetime.utcnow()
            self.total_successes += 1
        else:
            self.failures += 1
            self.total_failures += 1
            if self.failures >= 20:
                logging.warning('Deleting endpoint after failing %d times', self.failures)
                account_key = self.key.parent()
                self.key.delete()
                AccountEvent.create(account_key, 'Deleted Device',
                                    event_class='warning',
                                    properties={'UserAgent': self.device_info})
                return
            else:
                logging.warning('This endpoint has now failed %d time(s)', self.failures)
        self.populate(**kwargs)
        self.put()


class ExportedContent(ndb.Model):
    account = ndb.KeyProperty(Account, required=True)
    attachments = ndb.LocalStructuredProperty(ChunkAttachment, repeated=True)
    auth = ndb.KeyProperty(kind='ServiceAuth')
    chunk = ndb.KeyProperty(Chunk)
    destination_id = ndb.StringProperty()
    duration = ndb.IntegerProperty()
    next_content = ndb.KeyProperty(kind='ExportedContent')
    prev_content = ndb.KeyProperty(kind='ExportedContent')
    properties = ndb.JsonProperty()
    timestamp = ndb.DateTimeProperty(required=True)
    url = ndb.StringProperty()

    @classmethod
    def create(cls, content_id=None, **kwargs):
        if not content_id:
            content_id = cls.random_id()
        kwargs.setdefault('properties', {})
        content = cls(id=content_id, **kwargs)
        content.put()
        return content

    @classmethod
    def random_id(cls):
        return random.base62(21)


class Identity(ndb.Model, StatusMixin):
    account = ndb.KeyProperty(Account)
    available = ndb.BooleanProperty(indexed=False)
    status = ndb.StringProperty()

    @property
    def account_id(self):
        return self.account.key.id()

    @classmethod
    @ndb.transactional(xg=True)
    def add(cls, identifier, account_key, primary=False, **kwargs):
        account = account_key.get()
        if not account:
            logging.debug('Could not add %r to %d (account deleted)', identifier, account_key.id())
            return None, None
        new = cls.claim(identifier, **kwargs)
        if not new:
            logging.debug('Could not add %r to %d (claim failed)', identifier, account_key.id())
            return None, None
        new.account = account_key
        new.status = account.status
        if primary:
            account.identifiers.insert(0, new.key)
            account.primary_set = True
        else:
            account.identifiers.append(new.key)
        logging.debug('Added %r to %d', new.key.id(), account.key.id())
        ndb.put_multi([new, account])
        return new, account

    @classmethod
    @ndb.transactional(xg=True)
    def change(cls, from_id, to_id, assert_account_key=None, primary=False):
        # Reserve the new identifier before making any changes.
        new = cls.claim(to_id)
        if not new:
            return None, None
        entities = [new]
        # Release the old identity.
        account = cls.release(from_id, assert_account_key)
        # Move the old information over to the new identity.
        if account:
            entities.append(account)
            new.account = account.key
            new.status = account.status
            # Also update the identity of the attached account (if any).
            if primary:
                account.identifiers.insert(0, new.key)
                account.primary_set = True
            else:
                account.identifiers.append(new.key)
            logging.debug('Removed %r, added %r to account %s',
                          from_id, new.key.id(), account.key.id())
        else:
            new.account = None
            new.status = None
        ndb.put_multi(entities)
        return new, account

    @classmethod
    @ndb.transactional(xg=True)
    def claim(cls, identifier, reclaim=False, **kwargs):
        try:
            identifier = cls.validate(identifier, **kwargs)
        except errors.Reserved:
            logging.debug('Attempted to claim reserved identifier %r', identifier)
            return None
        identity = cls.get_by_id(identifier)
        if identity:
            if identity.available:
                identity.available = False
            elif reclaim or (identity.account and not identity.is_activated):
                # Reclaim the identity if it's connected to a non-activated account.
                logging.debug('Reclaiming identifier %r', identifier)
                account = identity.account.get()
                account.identifiers.remove(identity.key)
                account.put()
                identity.account = None
                identity.status = None
            else:
                return None
        else:
            identity = cls(id=identifier)
        identity.put()
        logging.debug('Claimed identifier %r', identifier)
        return identity

    @classmethod
    @ndb.transactional(xg=True)
    def release(cls, identifier, assert_account_key=None):
        identity = cls.get_by_id(identifier)
        if not identity or identity.available:
            logging.debug('Attempted to release unused identifier %r', identifier)
            return None
        account_key = identity.account
        if assert_account_key and account_key != assert_account_key:
            raise errors.ForbiddenAction('The identifier belongs to another account')
        identity.available = True
        identity.account = None
        identity.status = None
        identity.put()
        # Remove the identifier from the associated account (if any).
        if not account_key:
            logging.debug('Freed identifier %r (no account)', identifier)
            return None
        account = account_key.get()
        if not account:
            logging.debug('Freed identifier %r (account deleted)', identifier)
            return None
        if identity.key in account.identifiers:
            account.identifiers.remove(identity.key)
        else:
            logging.error('Tried to remove %r but it was not there', identifier)
        account.put()
        logging.debug('Freed identifier %r (account %s)', identifier, account_key.id())
        return account

    @classmethod
    def validate(cls, identifier, ignore_reserved=False):
        identifier, identifier_type = identifiers.parse(identifier)
        if not ignore_reserved and identifier in config.INVALID_IDENTIFIERS:
            raise errors.Reserved('That identifier is reserved')
        if identifier_type not in config.CLAIMABLE_IDENTIFIER_TYPES:
            raise errors.InvalidArgument('Invalid identifier type')
        return identifier


class Participant(ndb.Model):
    account = ndb.KeyProperty(Account, required=True)
    joined = ndb.DateTimeProperty(auto_now_add=True, indexed=False)
    last_chunk_end = ndb.DateTimeProperty(auto_now_add=True, indexed=False)
    last_played_from = ndb.DateTimeProperty(auto_now_add=True, indexed=False)
    owner = ndb.KeyProperty(Account, indexed=False)
    played_until = ndb.DateTimeProperty(auto_now_add=True, indexed=False)
    played_until_changed = ndb.DateTimeProperty(auto_now_add=True, indexed=False)
    total_duration = ndb.IntegerProperty(default=0, indexed=False)

    def played_chunk(self, chunk, count_self=False):
        if self.account == chunk.sender:
            return count_self
        return self.played_until >= chunk.end

    def set_played_until(self, new_value):
        if new_value <= self.played_until:
            return
        self.last_played_from = self.played_until
        self.played_until = new_value
        self.played_until_changed = datetime.utcnow()


class PasswordAuth(ndb.Model):
    """
    Data structure for verifying a user's password.

    The key name will be the key id of the corresponding account.
    """
    hash = ndb.BlobProperty(indexed=False)
    salt = ndb.BlobProperty(indexed=False)
    updated = ndb.DateTimeProperty(auto_now=True, indexed=False)


class Service(ndb.Model):
    account = ndb.KeyProperty(Account)
    categories = ndb.StringProperty('category', repeated=True)
    client_code = ndb.BooleanProperty(default=False, required=True)
    connect_url = ndb.StringProperty(indexed=False)
    description = ndb.StringProperty(indexed=False)
    featured = ndb.IntegerProperty()
    finish_pattern = ndb.StringProperty(indexed=False)
    image_url = ndb.StringProperty(indexed=False)
    title = ndb.StringProperty(indexed=False, required=True)

    _id_tuple = collections.namedtuple('ServiceIdentifier', 'service team resource')

    @classmethod
    def build_identifier(cls, service, team, resource):
        """Returns a string identifier. Team can be empty."""
        service_key = cls.resolve_key(service)
        if team:
            team_id = ServiceTeam.resolve_key((service_key, team)).id()
        else:
            team_id = None
        return identifiers.build_service(service_key.id(), team_id, resource)

    @classmethod
    def parse_identifier(cls, identifier):
        """Returns tuple (service, team, identifier)."""
        service, team, resource = identifiers.parse_service(identifier)
        service_key = cls.resolve_key(service)
        if team:
            team_key = ServiceTeam.resolve_key((service_key, team))
        else:
            team_key = None
        return cls._id_tuple(service_key, team_key, resource)

    @classmethod
    def parse_team(cls, identifier):
        """Returns either a tuple with service/team keys or None."""
        service_key, team_key, _ = cls.parse_identifier(identifier)
        if not team_key:
            return None
        return (service_key, team_key)

    @classmethod
    def resolve_key(cls, value):
        if isinstance(value, cls):
            return value.key
        elif isinstance(value, ndb.Key) and value.kind() == cls._get_kind():
            return value
        elif isinstance(value, basestring):
            return ndb.Key(cls, value)
        raise TypeError('Expected a Service key')


class ServiceAuth(ndb.Model):
    """Service authentication details for a single user."""
    access_token = ndb.StringProperty(indexed=False)
    expires_in = ndb.IntegerProperty(indexed=False)
    last_refresh = ndb.DateTimeProperty(auto_now_add=True)
    properties = ndb.JsonProperty(default={})
    refresh_token = ndb.StringProperty(indexed=False)
    service = ndb.KeyProperty(Service, required=True)
    service_clients = ndb.KeyProperty(kind='ServiceClient', indexed=False, repeated=True)
    service_team = ndb.KeyProperty(kind='ServiceTeam')
    service_identifier = ndb.StringProperty()
    token_type = ndb.StringProperty(indexed=False)

    def build_identifier(self, resource):
        return Service.build_identifier(self.service, self.service_team, resource)

    @property
    def identifier(self):
        return self.build_identifier(self.service_identifier)

    @classmethod
    def resolve_key(cls, value):
        if isinstance(value, cls):
            return value.key
        elif isinstance(value, ndb.Key) and value.kind() == cls._get_kind():
            return value
        elif isinstance(value, basestring):
            # The key id doesn't contain the account id.
            raise ValueError('insufficient information')
        elif isinstance(value, (list, tuple)):
            account, service, team = value
            service_key = Service.resolve_key(service)
            if team:
                team_key = ServiceTeam.resolve_key((service_key, team))
                key_name = '%s:%s' % (service_key.id(), team_key.id())
            else:
                key_name = service_key.id()
            account_key = Account.resolve_key(account)
            if not account_key:
                raise ValueError('invalid account')
            return ndb.Key(cls, key_name, parent=account_key)
        raise TypeError('Expected a ServiceAuth key')

    @classmethod
    def split_key(cls, value):
        """Returns tuple (account, service, team)."""
        key = cls.resolve_key(value)
        service, _, team = key.id().partition(':')
        service_key = Service.resolve_key(service)
        team_key = ServiceTeam.resolve_key((service_key, team)) if team else None
        return key.parent(), service_key, team_key


class ServiceClient(ndb.Model):
    client_secret = ndb.StringProperty(indexed=False, required=True)
    description = ndb.StringProperty(indexed=False)
    image_url = ndb.StringProperty(indexed=False)
    redirect_uris = ndb.StringProperty(indexed=False, repeated=True)
    title = ndb.StringProperty(indexed=False, required=True)


class ServiceTeam(ndb.Model):
    created = ndb.DateTimeProperty(auto_now_add=True)
    image = ndb.StringProperty(indexed=False)
    slug = ndb.StringProperty()
    stored_name = ndb.StringProperty('name', indexed=False)
    whitelisted = ndb.BooleanProperty()

    def build_identifier(self, resource):
        return Service.build_identifier(self.key.parent(), self.key, resource)

    @classmethod
    @ndb.transactional
    def create_or_update(cls, team_key, **kwargs):
        team = team_key.get()
        if team:
            if team.to_dict(include=kwargs.keys()) == kwargs:
                # No change.
                return team
            team.populate(**kwargs)
        else:
            team = cls(key=team_key, **kwargs)
        team.put()
        return team

    def get_name(self):
        return self.stored_name or self.key.id()
    def set_name(self, value):
        self.stored_name = value
    name = property(get_name, set_name)

    @property
    def image_url(self):
        return files.storage_url(self.image)

    def public(self, api_version=None, **kwargs):
        return {
            'image_url': self.image_url,
            'name': self.name,
        }

    @classmethod
    def resolve_key(cls, value):
        if isinstance(value, cls):
            return value.key
        elif isinstance(value, ndb.Key) and value.kind() == cls._get_kind():
            return value
        elif isinstance(value, basestring):
            # The key id doesn't contain the service id.
            raise ValueError('insufficient information')
        elif isinstance(value, (list, tuple)):
            service, team = value
            service_key = Service.resolve_key(service)
            if isinstance(team, ndb.Key) and team.kind() == cls._get_kind():
                if team.parent() != service_key:
                    raise ValueError('team service mismatch')
                team = team.id()
            elif not isinstance(team, basestring):
                # TODO: More validation.
                raise TypeError('invalid team id value')
            return ndb.Key(cls, team, parent=service_key)
        raise TypeError('Expected a ServiceTeam key')

    @property
    def slug_with_fallback(self):
        return self.slug or re.sub(r'[^a-zA-Z0-9]+', '', self.name).lower()

    def _set_attributes(self, attrs):
        if 'name' in attrs:
            attrs['stored_name'] = attrs.pop('name')
        super(ServiceTeam, self)._set_attributes(attrs)


class Stream(ndb.Model):
    attachments = ndb.LocalStructuredProperty(Attachment, repeated=True)
    chunks = ndb.LocalStructuredProperty(ChunkInStream, repeated=True)
    created = ndb.DateTimeProperty(auto_now_add=True)
    export_content_disabled = ndb.BooleanProperty(default=False, indexed=False)
    featured = ndb.IntegerProperty()
    has_been_featured = ndb.BooleanProperty()
    image = ndb.StringProperty('image_url', indexed=False)
    index = ndb.BlobProperty(indexed=True)
    invite_token = ndb.StringProperty()
    language = ndb.StringProperty(indexed=False)
    last_interaction = ndb.DateTimeProperty(auto_now_add=True)
    not_played_by = ndb.KeyProperty(Account, repeated=True)
    participants = ndb.StructuredProperty(Participant, repeated=True)
    service = ndb.KeyProperty(Service, indexed=False)
    service_content_id = ndb.StringProperty()
    service_members = ndb.StringProperty(indexed=False, repeated=True)
    service_owner = ndb.KeyProperty(Account, indexed=False)
    solo = ndb.BooleanProperty(default=False, indexed=False)
    title = ndb.StringProperty(indexed=False)
    total_duration = ndb.IntegerProperty(default=0)
    visible_by = ndb.KeyProperty(Account, repeated=True)

    @classmethod
    @ndb.transactional
    def add_chunk(cls, stream_key, sender, payload, duration, allow_duplicate=False,
                  mark_older_played=False, persist=False, show_for_sender=True,
                  start_now=False, token=None, **kwargs):
        if token is not None and not re.match('^[a-zA-Z0-9]+$', token):
            raise errors.InvalidArgument('Invalid chunk token (expected base62)')
        stream = stream_key.get()
        if not stream:
            raise errors.ResourceNotFound('Stream does not exist')
        # Find the Participant model for the sender.
        for participant in stream.participants:
            if participant.account == sender:
                break
        else:
            raise errors.ForbiddenAction('Account is not a participant in stream')
        # Don't allow identical audio data twice in a row from the same user.
        # XXX: Anyone talking to Share is exempted.
        share = ndb.Key('Account', 355150003)
        if not allow_duplicate and share not in stream.participants:
            for c in reversed(stream.chunks):
                if c.sender == sender:
                    if c.payload == payload:
                        raise errors.ForbiddenAction('Duplicate content')
                    break
        # Create the chunk and store it.
        delta = timedelta(milliseconds=duration)
        if start_now:
            # Anchor the start timestamp to now instead of the end.
            start = datetime.utcnow()
            end = start + delta
        else:
            end = datetime.utcnow()
            prev_chunk = stream.chunks[-1] if stream.chunks else None
            if prev_chunk:
                # Ensure that chunk doesn't start before the previous one ends.
                # TODO: Technically this is incorrect but in the future we need to stream
                #       the audio in which case we'll create a chunk with start but no end
                #       (until the audio stream has completed).
                end = max(prev_chunk.end + delta + timedelta(milliseconds=1), end)
            start = end - delta
        # Store the chunk entity in the datastore.
        chunk = Chunk(parent=stream_key, end=end, payload=payload, persist=persist,
                      sender=sender, start=start, token=token, **kwargs)
        chunk.put()
        # Move the sending participant to the beginning of the list.
        stream.participants.remove(participant)
        stream.participants.insert(0, participant)
        # Update the last_chunk_end property for all other participants.
        for p in stream.participants:
            # If specified, mark older chunks as played by everyone.
            if mark_older_played:
                p.played_until = p.last_chunk_end
            # Skip sender unless this is a solo stream.
            if p.account == sender and not stream.solo:
                continue
            # Update the end timestamp of the stream for this participant.
            p.last_chunk_end = end
        # Create a set of all the participating accounts' keys for the code below.
        participating_account_keys = Account.resolve_keys(stream.participants)
        # Update played state for the participants.
        unplayed_set = participating_account_keys.copy()
        # Keep the stream as played for the sender if they had already played.
        # Solo streams always get marked unplayed.
        if not stream.solo and sender not in stream.not_played_by or mark_older_played:
            unplayed_set.discard(sender)
        stream.not_played_by = list(unplayed_set)
        # Make the stream visible by all the participants.
        visible_by_set = participating_account_keys.copy()
        if not show_for_sender and sender not in stream.visible_by:
            # Don't show the stream for the sender if show_for_sender=False.
            visible_by_set.discard(sender)
        stream.visible_by = list(visible_by_set)
        # Keep the last 10 chunks stored together within the stream entity.
        local_chunk = ChunkInStream.from_chunk(chunk)
        stream.chunks = stream.chunks[-9:] + [local_chunk]
        # Update the timestamps and metrics of the stream.
        stream.last_interaction = end
        stream.total_duration += chunk.duration
        participant.total_duration += chunk.duration
        # TODO: Remove this once we've migrated to new index code.
        stream._ensure_index()
        stream.put()
        return stream

    def build_index(self):
        return self._build_index(self.participant_keys, title=self.title)

    @classmethod
    def generate_invite_token(cls):
        return random.base62(12)

    @classmethod
    def get(cls, participants, solo=False, title=title):
        account_keys = Account.resolve_keys(participants)
        if solo and len(account_keys) != 1:
            raise errors.InvalidArgument('Solo streams may only have one participant')
        index = cls._build_index(account_keys, title=title)
        stream = cls.query(cls.index == index).get()
        if stream and stream.solo != solo:
            # This shouldn't be happening unless the stream is old.
            stream.solo = solo
            stream.put()
        return stream

    @classmethod
    def get_by_id_with_chunks(cls, stream_id):
        stream_key = ndb.Key(cls, stream_id)
        # Load chunks and stream in parallel.
        threshold = datetime.utcnow() - config.CHUNK_MAX_AGE
        q = Chunk.query(Chunk.start >= threshold, ancestor=stream_key).order(Chunk.start)
        chunks_future = q.fetch_async()
        return stream_key.get(), chunks_future.get_result()

    @classmethod
    def get_or_create(cls, title, participants, create_hidden=False,
                      owners=None, shareable=False, solo=False, **kwargs):
        account_keys = Account.resolve_keys(participants)
        if solo and title:
            raise errors.InvalidArgument('Solo streams cannot have a title')
        if solo and len(account_keys) != 1:
            raise errors.InvalidArgument('Solo streams may only have one participant')
        index_bytes = cls._build_index(account_keys, title=title)
        if len(account_keys) > 1 or solo:
            # Attempt to look for an existing stream with the desired configuration.
            stream = cls.query(cls.index == index_bytes).get()
            if stream:
                if stream.solo != solo:
                    # This shouldn't be happening unless the stream is old.
                    stream.solo = solo
                    stream.put()
                return stream, False
        # We need to create the stream. Ensure all the accounts exist.
        if all(isinstance(p, Account) for p in participants):
            # We were provided only account instances, so assume everything is valid.
            pass
        elif not cls._validate_accounts(account_keys):
            raise errors.InvalidArgument('Got one or more invalid account(s)')
        stream = cls(index=index_bytes, solo=solo, title=title, **kwargs)
        for k in account_keys:
            p = Participant(account=k, owner=owners.get(k) if owners else None)
            stream.participants.append(p)
        if not create_hidden:
            stream.visible_by = account_keys
        if shareable:
            stream.invite_token = cls.generate_invite_token()
        stream.put()
        return stream, True

    @property
    def image_url(self):
        return files.storage_url(self.image)

    @property
    def participant_keys(self):
        return {p.account for p in self.participants}

    @classmethod
    @ndb.transactional
    def remove_attachment(cls, stream_key, account_key, attachment_id):
        stream = stream_key.get()
        if not stream:
            raise errors.ResourceNotFound('Stream does not exist')
        for index, attachment in enumerate(stream.attachments):
            if attachment.id == attachment_id:
                break
        else:
            raise errors.ResourceNotFound('Attachment does not exist')
        stream.attachments.pop(index)
        stream.put()
        return stream

    @classmethod
    @ndb.transactional
    def remove_chunk(cls, stream_key, chunk_id):
        stream = stream_key.get()
        if not stream:
            raise errors.ResourceNotFound('Stream does not exist')
        for index, chunk in enumerate(stream.chunks):
            if chunk.chunk_id == chunk_id:
                # Remove the chunk in the stream.
                # TODO: Consider side effects if this is the last chunk.
                stream.chunks.pop(index)
                stream.put()
                break
        chunk_key = ndb.Key('Stream', stream_key.id(), 'Chunk', chunk_id)
        chunk_key.delete()
        return stream

    @classmethod
    @ndb.transactional
    def set_attachment(cls, stream_key, account_key, attachment_id, do_not_bump=False, **kwargs):
        stream = stream_key.get()
        if not stream:
            raise errors.ResourceNotFound('Stream does not exist')
        for attachment in stream.attachments:
            if attachment.id == attachment_id:
                # Updating an existing attachment.
                # TODO: Decide if we should disallow changing attachment type.
                break
        else:
            # TODO: Adjust this limitation.
            if len(stream.attachments) > 5:
                raise errors.ForbiddenAction('Stream cannot have any more attachments')
            attachment = Attachment(id=attachment_id)
            stream.attachments.append(attachment)
        attachment.populate(**kwargs)
        if not attachment.type:
            raise errors.InvalidArgument('Attachment property "type" is required')
        # TODO: Decide if the account is overridable.
        # TODO: Consider moving most recently modified attachment to top.
        attachment.account = account_key
        attachment.timestamp = datetime.utcnow()
        if not do_not_bump:
            stream.last_interaction = datetime.utcnow()
        stream.put()
        return stream

    @classmethod
    @ndb.transactional
    def set_chunk_reaction(cls, chunk_key, account_key, reaction_type):
        stream, chunk = ndb.get_multi([chunk_key.parent(), chunk_key])
        if not stream or not chunk:
            raise errors.ResourceNotFound('Stream or chunk does not exist')
        # Add or remove the account from the reactions list based on reaction.
        chunk.set_reaction(account_key, reaction_type)
        # Also update cached chunk if the stream entity has it.
        entities = [chunk]
        for i, c in enumerate(stream.chunks):
            if c.chunk_id == chunk_key.id():
                stream.chunks[i] = ChunkInStream.from_chunk(chunk)
                entities.append(stream)
                break
        ndb.put_multi(entities)
        return stream, chunk

    @classmethod
    @ndb.transactional
    def set_chunk_text(cls, chunk_key, text_segments, **kwargs):
        # TODO: Support updating text segments as well.
        stream, chunk = ndb.get_multi([chunk_key.parent(), chunk_key])
        if not stream or not chunk:
            raise errors.ResourceNotFound('Stream or chunk does not exist')
        chunk.text_segments = text_segments
        # Also update cached chunk if the stream entity has it.
        entities = [chunk]
        for i, c in enumerate(stream.chunks):
            if c.chunk_id == chunk_key.id():
                stream.chunks[i] = ChunkInStream.from_chunk(chunk)
                entities.append(stream)
                break
        ndb.put_multi(entities)
        return stream, chunk

    @classmethod
    @ndb.transactional
    def set_featured(cls, stream_key, featured):
        if not isinstance(featured, (int, long)) or featured < 0:
            raise errors.InvalidArgument('Invalid featured value')
        stream = stream_key.get()
        if featured == stream.featured:
            # No change.
            return stream
        stream.featured = featured
        stream.has_been_featured = not featured
        stream.put()
        return stream

    @classmethod
    @ndb.transactional
    def set_image(cls, stream_key, image):
        stream = stream_key.get()
        if not stream:
            raise errors.ResourceNotFound('Stream does not exist')
        stream.image = image
        # TODO: Remove this once we've migrated to new index code.
        stream._ensure_index()
        stream.put()
        return stream

    @classmethod
    @ndb.transactional
    def set_last_interaction(cls, stream_key, last_interaction):
        stream = stream_key.get()
        if not stream:
            raise errors.ResourceNotFound('Stream does not exist')
        if stream.last_interaction > last_interaction:
            # If the current last interaction timestamp is more recent, do nothing.
            return stream
        stream.last_interaction = last_interaction
        # TODO: Remove this once we've migrated to new index code.
        stream._ensure_index()
        stream.put()
        return stream

    @classmethod
    def set_participants(cls, stream_key, add=None, remove=None, **kwargs):
        """Add or remove participants to/from the stream."""
        # Check the add list outside of a transaction to prevent hitting limit.
        if add:
            add = Account.resolve_keys(add)
            # Ensure all the accounts exist.
            if not cls._validate_accounts(add):
                raise errors.InvalidArgument('Got one or more invalid account(s)')
        return cls._set_participants(stream_key, add, remove, **kwargs)

    @classmethod
    @ndb.transactional
    def set_played_until(cls, stream_key, account_key, played_until):
        stream = stream_key.get()
        if not stream:
            raise errors.ResourceNotFound('Stream does not exist')
        # Find the Participant instance for the account.
        for participant in stream.participants:
            if participant.account == account_key:
                break
        else:
            raise errors.InvalidArgument('Account is not a participant in stream')
        if played_until < participant.played_until:
            # Can't unplay something.
            return stream
        # Ensure that the timestamp isn't in the future.
        if played_until > participant.last_chunk_end:
            error = 'Cannot set played_until to {} (past last_chunk_end: {})'.format(
                played_until, participant.last_chunk_end)
            raise errors.InvalidArgument(error)
        participant.set_played_until(played_until)
        # Mark the stream as played if the last chunk has been played.
        if played_until == participant.last_chunk_end:
            try:
                stream.not_played_by.remove(account_key)
            except ValueError:
                pass
        # TODO: Remove this once we've migrated to new index code.
        stream._ensure_index()
        stream.put()
        return stream

    @classmethod
    @ndb.transactional
    def set_shareable(cls, stream_key, shareable):
        stream = stream_key.get()
        if bool(shareable) == stream.shareable:
            # No change.
            return stream
        stream.invite_token = cls.generate_invite_token() if shareable else None
        # TODO: Remove this once we've migrated to new index code.
        stream._ensure_index()
        stream.put()
        return stream

    @classmethod
    @ndb.transactional(xg=True)
    def set_title(cls, stream_key, new_title):
        if not new_title:
            # Ensure that empty string becomes no title.
            new_title = None
        stream = stream_key.get()
        if not stream:
            raise errors.ResourceNotFound('Stream does not exist')
        if stream.solo:
            raise errors.ForbiddenAction('Solo streams may not have their title changed')
        # Exit early if the title didn't change.
        if stream.title == new_title:
            return stream
        # Update the stream entity.
        stream.title = new_title
        stream.index = stream.build_index()
        stream.put()
        return stream

    @classmethod
    @ndb.transactional
    def set_visibility(cls, stream_key, show_for=None, hide_for=None):
        """Changes the visibility of a stream using a delta of accounts to show or hide
        the stream for (so that race conditions can be resolved more or less correctly).

        """
        stream = stream_key.get()
        if not stream:
            raise errors.ResourceNotFound('Stream does not exist')
        # Add to, remove from, and validate (against participants) the visibility list.
        visible_by = set(stream.visible_by)
        if show_for:
            visible_by |= set(show_for)
        if hide_for:
            visible_by -= set(hide_for)
        stream.visible_by = list(visible_by & Account.resolve_keys(stream.participants))
        # TODO: Remove this once we've migrated to new index code.
        stream._ensure_index()
        stream.put()
        return stream

    @property
    def shareable(self):
        return bool(self.invite_token)

    @classmethod
    def _build_index(cls, participants, title=None):
        # TODO: Return index in binary instead of base64.
        # Bytes used with binary (max num_keys = 185):
        # 1 + num_keys * 8 + 16
        # Bytes used with base64 (max num_keys = 138):
        # 4 * ceil((1 + num_keys * 8 + 16) / 3)
        account_keys = list(Account.resolve_keys(participants))
        if not account_keys:
            return None
        if not all(account_keys):
            raise errors.InvalidArgument('Failed to resolve all accounts')
        # Limit the number of keys for performance and storage reasons.
        num_keys = len(account_keys)
        if num_keys > 100:
            raise errors.InvalidArgument('There may only be up to 100 participants')
        # Keys need to be in a deterministic order.
        account_keys.sort()
        # Create a binary key of count + keys.
        key_format = '!B' + 'Q' * num_keys
        try:
            name = struct.pack(key_format, num_keys, *(key.id() for key in account_keys))
        except:
            logging.warning('Failed to pack keys: %r', account_keys)
            raise
        if title:
            name += hashlib.md5(title.encode('utf8')).digest()
        return base64.b64encode(name)

    def _ensure_index(self):
        if self.index:
            return
        self.index = self.build_index()

    @classmethod
    @ndb.transactional(xg=True)
    def _set_participants(cls, stream_key, add, remove, do_not_bump=False, owners=None):
        stream = stream_key.get()
        if not stream:
            raise errors.ResourceNotFound('Stream does not exist')
        if stream.solo:
            raise errors.ForbiddenAction('Cannot change participants of this stream')
        from_keys = Account.resolve_keys(stream.participants)
        to_keys = from_keys.copy()
        if add:
            to_keys |= Account.resolve_keys(add)
        if remove:
            to_keys -= Account.resolve_keys(remove)
        # Make sure there are no bots belonging to any of the removed participants.
        to_keys -= set(p.account for p in stream.participants
                       if p.owner and p.owner not in to_keys)
        if from_keys == to_keys:
            # If the participant list didn't change, do nothing.
            return stream
        new_keys = to_keys - from_keys
        logging.debug('Stream participants change: +[%s] -[%s]',
                      ', '.join(str(k.id()) for k in new_keys),
                      ', '.join(str(k.id()) for k in from_keys - to_keys))
        extra_args = {}
        if new_keys:
            # Try to leave a few chunks unplayed for new participants.
            unplayed_duration = 0
            for chunk in reversed(stream.chunks):
                if chunk.is_expired:
                    break
                ts = chunk.start - timedelta.resolution
                extra_args.update(last_played_from=ts, played_until=ts)
                unplayed_duration += chunk.duration
                if unplayed_duration >= 120000:
                    # Don't accumulate more than two minutes of unplayed content.
                    break
        # Store the stream with the new set of participants, keeping old data.
        stream.participants = [p for p in stream.participants if p.account in to_keys]
        for k in new_keys:
            p = Participant(account=k, **extra_args)
            if owners:
                p.owner = owners.get(k)
            stream.participants.append(p)
        if not do_not_bump:
            # Update the last interaction timestamp on the stream.
            stream.last_interaction = datetime.utcnow()
        # Hide the stream for the departed and show it for joining participants.
        stream.visible_by = list(set(stream.visible_by) & to_keys | new_keys)
        if 'played_until' in extra_args:
            # The stream will also be unplayed by the new participants.
            stream.not_played_by = list(set(stream.not_played_by) & to_keys | new_keys)
        else:
            stream.not_played_by = list(set(stream.not_played_by) & to_keys)
        stream.index = stream.build_index()
        stream.put()
        return stream

    @classmethod
    def _validate_accounts(cls, accounts):
        try:
            # This will throw if any of the accounts don't exist.
            Account.resolve_list(accounts)
        except ValueError:
            return False
        return True


class ThreadAccount(ndb.Model):
    account = ndb.KeyProperty(Account, indexed=False, required=True)
    image_url = ndb.StringProperty(indexed=False)
    joined = ndb.DateTimeProperty(auto_now_add=True, indexed=False, required=True)
    seen_until = ndb.KeyProperty(indexed=False, kind='ThreadMessage')
    seen_until_timestamp = ndb.DateTimeProperty(auto_now_add=True, indexed=False, required=True)
    username = ndb.StringProperty(indexed=False)
    verified = ndb.BooleanProperty(indexed=False)

    @classmethod
    def from_account(cls, account):
        return ThreadAccount(account=account.key,
                             image_url=account.image_url,
                             username=account.username,
                             verified=account.verified)

    def public(self, version=None, **kwargs):
        data = {
            'id': self.account.id(),
            'image_url': self.image_url,
            'joined': self.joined,
            'seen_until': self.seen_until.id() if self.seen_until else None,
            'seen_until_timestamp': self.seen_until_timestamp,
            'username': self.username,
        }
        if version >= 51:
            data['verified'] = self.verified or False
        return data

    def update(self, account):
        if not isinstance(account, Account) or account.key != self.account:
            raise ValueError('Invalid account value')
        self.image_url = account.image_url
        self.username = account.username
        self.verified = account.verified


class ThreadMessage(ndb.Model):
    TYPES = ['currency', 'request', 'text']
    TYPES_SETTABLE_BY_USER = ['text']

    account = ndb.KeyProperty(Account, indexed=False, required=True)
    created = ndb.DateTimeProperty(required=True)
    data = ndb.JsonProperty(required=True)
    text = ndb.StringProperty(indexed=False, required=True)
    type = ndb.StringProperty(indexed=False, required=True)

    @classmethod
    def make_key(cls, thread_key, account_key, timestamp=None):
        name = '%s_%s' % (convert.unix_timestamp_ms(timestamp), account_key.id())
        return ndb.Key(cls._get_kind(), name, parent=thread_key)

    def public(self, **kwargs):
        return self.public_with_id(self.key.id(), **kwargs)

    def public_with_id(self, message_id, version=None, **kwargs):
        return {
            'id': message_id,
            'account_id': self.account.id(),
            'created': self.created,
            'data': self.data,
            'text': self.text,
            'type': self.type,
        }


class ThreadMessageCached(ThreadMessage):
    message_id = ndb.StringProperty(indexed=False, required=True)

    @classmethod
    def from_message(cls, message):
        props = dict((p._code_name, p.__get__(message, ThreadMessage))
                     for p in ThreadMessage._properties.itervalues()
                     if type(p) is not ndb.ComputedProperty)
        return cls(message_id=message.key.id(), **props)

    def key_with_parent(self, thread_key):
        return ndb.Key(ThreadMessage._get_kind(), self.message_id, parent=thread_key)

    def public(self, **kwargs):
        return self.public_with_id(self.message_id, **kwargs)


class Thread(ndb.Model):
    accounts = ndb.LocalStructuredProperty(ThreadAccount, repeated=True)
    created = ndb.DateTimeProperty(auto_now_add=True, indexed=False)
    last_interaction = ndb.DateTimeProperty(auto_now_add=True)
    messages = ndb.LocalStructuredProperty(ThreadMessageCached, repeated=True)
    visible_by = ndb.KeyProperty(Account, repeated=True)

    @classmethod
    @ndb.transactional_tasklet
    def add_message_async(cls, thread_key, account_key, type, text, data, account=None, allow_nonuser_type=False, keep_hidden_from_sender=False):
        if not isinstance(thread_key, ndb.Key) or thread_key.kind() != 'Thread':
            raise ValueError('Invalid Thread key')
        if not isinstance(account_key, ndb.Key) or account_key.kind() != 'Account':
            raise ValueError('Invalid Account key')
        if not isinstance(text, basestring):
            raise TypeError('Message text must be a string')
        thread = yield thread_key.get_async()
        if not thread:
            raise errors.ResourceNotFound('Thread not found')
        if account and any(a.account in account.blocked_by for a in thread.accounts):
            raise errors.ResourceNotFound('Thread not found')
        for index, thread_account in enumerate(thread.accounts):
            if thread_account.account == account_key:
                break
        else:
            raise errors.ResourceNotFound('Thread not found')
        if type not in ThreadMessage.TYPES:
            raise errors.InvalidArgument('Invalid message type')
        if not allow_nonuser_type and type not in ThreadMessage.TYPES_SETTABLE_BY_USER:
            raise errors.InvalidArgument('That message type may not be set by user')
        text = text.strip()
        if not text:
            raise errors.InvalidArgument('Invalid message text')
        if not isinstance(data, dict):
            raise errors.InvalidArgument('Invalid message data')
        now = datetime.utcnow()
        # Create the message that will be stored in permanent history.
        message_key = ThreadMessage.make_key(thread_key, account_key, now)
        message = ThreadMessage(key=message_key, account=account_key,
                                created=datetime.utcnow(),
                                data=data, text=text, type=type)
        # Insert a cached message object in the thread (maximum of 10).
        thread.messages = [ThreadMessageCached.from_message(message)] + thread.messages[:9]
        # Update the account object in the thread.
        del thread.accounts[index]
        thread_account.seen_until = message_key
        thread_account.seen_until_timestamp = now
        if account:
            # Update the ThreadAccount entity with new metadata.
            thread_account.update(account)
        thread.accounts.insert(0, thread_account)
        # Update the thread properties.
        thread.last_interaction = now
        hidden_from_sender = account_key not in thread.visible_by
        thread.visible_by = [a.account for a in thread.accounts]
        if keep_hidden_from_sender and hidden_from_sender:
            thread.visible_by.remove(account_key)
        # Store the thread and message objects, then return them.
        yield ndb.put_multi_async([thread, message])
        raise ndb.Return((thread, message))

    @classmethod
    @ndb.transactional_tasklet(xg=True)
    def lookup_async(cls, account_keys):
        thread_key = cls.make_key(account_keys)
        thread = yield thread_key.get_async()
        if thread:
            raise ndb.Return(thread)
        accounts = yield ndb.get_multi_async(account_keys)
        if not all(accounts):
            raise errors.ResourceNotFound('That account does not exist')
        # Collect all account keys that the participants have been blocked by.
        blocker_keys = set(k for a in accounts for k in a.blocked_by)
        if any(k in blocker_keys for k in account_keys):
            # One of the participants has been blocked by another participant.
            raise errors.ResourceNotFound('That account does not exist')
        thread = cls(key=thread_key)
        thread.accounts = [ThreadAccount.from_account(a) for a in accounts]
        yield thread.put_async()
        raise ndb.Return(thread)

    @classmethod
    def make_key(cls, account_keys):
        for k in account_keys:
            if not isinstance(k, ndb.Key) or k.kind() != 'Account':
                raise TypeError('Expected only Account keys')
        sorted_keys = sorted(set(account_keys))
        if len(sorted_keys) != len(account_keys):
            raise ValueError('Expected only unique Account keys')
        key_format = '!B' + 'Q' * len(sorted_keys)
        binary = struct.pack(key_format, len(sorted_keys), *(k.id() for k in sorted_keys))
        return ndb.Key('Thread', base64.urlsafe_b64encode(binary).rstrip('='))


class Wallet(ndb.Model):
    account = ndb.KeyProperty(Account, required=True)
    balance = ndb.IntegerProperty(default=0, required=True)
    comment = ndb.StringProperty(indexed=False, required=True)
    created = ndb.DateTimeProperty(auto_now_add=True, required=True)
    last_tx = ndb.StringProperty(default='0'*64, indexed=False)
    total_received = ndb.IntegerProperty(default=0, required=True)
    total_sent = ndb.IntegerProperty(default=0, required=True)
    updated = ndb.DateTimeProperty(auto_now=True, required=True)

    @classmethod
    def create(cls, *args, **kwargs):
        return cls.create_async(*args, **kwargs).get_result()

    @classmethod
    @ndb.transactional_tasklet(xg=True)
    def create_async(cls, account_key, attr='wallet'):
        if not isinstance(account_key, ndb.Key) or account_key.kind() != 'Account':
            raise TypeError('account_key must be an Account key')
        if attr == 'wallet':
            wallet_id = 'account_%d' % (account_key.id(),)
            initial_balance = 0
            comment = 'Wallet for %s' % (account_key.id(),)
        elif attr == 'wallet_bonus':
            wallet_id = 'account_%d_bonus' % (account_key.id(),)
            initial_balance = 10
            comment = 'Bonus pot for %s' % (account_key.id(),)
        else:
            raise ValueError('Invalid attribute %r' % (attr,))
        wallet_key = cls.key_from_id(wallet_id)
        account, wallet = yield ndb.get_multi_async([account_key, wallet_key])
        if getattr(account, attr):
            raise errors.InvalidArgument('Account already has a wallet')
        setattr(account, attr, wallet_key)
        if wallet:
            logging.debug('Account wallet exists but is not set on account (autofixing)')
            yield account.put_async()
        else:
            wallet = cls(key=wallet_key,
                         account=account_key,
                         balance=initial_balance,
                         comment=comment,
                         total_received=initial_balance)
            yield ndb.put_multi_async([account, wallet])
        raise ndb.Return((account, wallet))

    @classmethod
    def create_and_transfer(cls, *args, **kwargs):
        return cls.create_and_transfer_async(*args, **kwargs).get_result()

    @classmethod
    @ndb.tasklet
    def create_and_transfer_async(cls, account_key, dst_wallet_key, new_wallet_id, amount, comment):
        # Create the wallet that will hold the created currency.
        new_wallet = yield cls.create_internal_async(account_key, new_wallet_id, amount, comment)
        # Transfer all the currency to the destination wallet.
        tx = yield cls.create_tx_async(account_key, new_wallet.key, dst_wallet_key, amount, comment)
        _, _, wallet, _ = yield tx()
        # Return the updated destination wallet.
        raise ndb.Return(wallet)

    @classmethod
    def create_internal(cls, *args, **kwargs):
        return cls.create_internal_async(*args, **kwargs).get_result()

    @classmethod
    @ndb.transactional_tasklet
    def create_internal_async(cls, account_key, wallet_id, initial_balance, comment):
        wallet_key = cls.key_from_id(wallet_id)
        if not isinstance(account_key, ndb.Key) or account_key.kind() != 'Account':
            raise TypeError('account_key must be an Account key')
        if not isinstance(initial_balance, (int, long)) or initial_balance < 0:
            raise TypeError('initial_balance must be a non-negative integer')
        if not comment:
            raise ValueError('comment must be provided')
        existing_wallet = yield wallet_key.get_async()
        if existing_wallet:
            logging.error('Wallet id %r already exists (with account %d)',
                wallet_key.id(), existing_wallet.account.id())
            raise errors.AlreadyExists('Wallet id already exists')
        wallet = cls(key=wallet_key,
                     account=account_key,
                     balance=initial_balance,
                     comment=comment,
                     total_received=initial_balance)
        yield wallet.put_async()
        raise ndb.Return(wallet)

    @classmethod
    def create_tx(cls, *args, **kwargs):
        return cls.create_tx_async(*args, **kwargs).get_result()

    @classmethod
    @ndb.tasklet
    def create_tx_async(cls, account_key, w1_key, w2_key, amount, comment, require_full_amount=True):
        if not isinstance(account_key, ndb.Key) or account_key.kind() != 'Account':
            raise TypeError('account_key must be an Account key')
        if not isinstance(w1_key, ndb.Key) or w1_key.kind() != 'Wallet':
            raise TypeError('w1_key must be a Wallet key')
        if not isinstance(w2_key, ndb.Key) or w2_key.kind() != 'Wallet':
            raise TypeError('w2_key must be a Wallet key')
        if not isinstance(amount, (int, long)) or amount < 1:
            raise TypeError('amount must be a positive integer')
        if not isinstance(comment, basestring) or not len(comment):
            raise TypeError('comment must be a valid string')
        if w1_key == w2_key:
            raise ValueError('Do not transfer to same wallet')
        w1, w2 = yield ndb.get_multi_async([w1_key, w2_key])
        if not w1:
            raise ValueError('w1_key is not a valid Wallet key')
        if not w2:
            raise ValueError('w2_key is not a valid Wallet key')
        if w1.account != account_key:
            raise ValueError('w1 must be owned by account_key')
        if (require_full_amount and w1.balance < amount) or not w1.balance:
            @ndb.tasklet
            def tx():
                raise WalletInsufficientFunds()
            raise ndb.Return(tx)
        @ndb.transactional_tasklet(xg=True)
        def tx():
            w1_, w2_ = yield ndb.get_multi_async([w1_key, w2_key])
            if (w1_.last_tx, w2_.last_tx) != (w1.last_tx, w2.last_tx):
                raise WalletChanged()
            if not require_full_amount and w1_.balance < amount:
                amount_ = w1_.balance
                if not amount_:
                    raise WalletInsufficientFunds()
            else:
                amount_ = amount
            if w1_.balance < amount_:
                raise WalletInsufficientFunds()
            tx1_hash = WalletTransaction.make_hash(w1_, w1_.account, w2_.account, -amount_)
            w1_.last_tx = tx1_hash
            tx1_key = ndb.Key(WalletTransaction, tx1_hash, parent=w1_key)
            tx2_hash = WalletTransaction.make_hash(w2_, w1_.account, w2_.account, amount_)
            w2_.last_tx = tx2_hash
            tx2_key = ndb.Key(WalletTransaction, tx2_hash, parent=w2_key)
            tx1 = WalletTransaction(key=tx1_key, delta=-amount_, comment=comment, other_tx=tx2_key,
                                    old_balance=w1_.balance, new_balance=w1_.balance - amount_,
                                    sender=w1_.account, receiver=w2_.account)
            w1_.balance = tx1.new_balance
            w1_.total_sent += amount_
            tx2 = WalletTransaction(key=tx2_key, delta=amount_, comment=comment, other_tx=tx1_key,
                                    old_balance=w2_.balance, new_balance=w2_.balance + amount_,
                                    sender=w1_.account, receiver=w2_.account)
            w2_.balance = tx2.new_balance
            w2_.total_received += amount_
            yield ndb.put_multi_async([tx1, tx2, w1_, w2_])
            if w1.balance + w2.balance != w1_.balance + w2_.balance:
                raise errors.ServerError('Balance mismatch')
            if w1_.total_received - w1_.total_sent != w1_.balance:
                raise errors.ServerError('Balance mismatch')
            if w2_.total_received - w2_.total_sent != w2_.balance:
                raise errors.ServerError('Balance mismatch')
            raise ndb.Return((w1_, tx1, w2_, tx2))
        raise ndb.Return(tx)

    @classmethod
    def key_from_id(cls, wallet_id):
        if not isinstance(wallet_id, basestring) or not wallet_id:
            raise TypeError('wallet_id must be a non-empty string')
        # Only allow ASCII characters in the id.
        wallet_id = wallet_id.encode('ascii')
        return ndb.Key(Wallet, wallet_id)

    def public(self, version=None, **kwargs):
        return {
            'id': self.key.id(),
            'balance': self.balance,
            'created': self.created,
        }


class WalletChanged(Exception):
    def __init__(self):
        super(WalletChanged, self).__init__('Another transaction occurred, please try again')


class WalletInsufficientFunds(errors.InvalidArgument):
    def __init__(self):
        super(WalletInsufficientFunds, self).__init__('Insufficient funds')


class WalletTransaction(ndb.Model):
    comment = ndb.StringProperty(indexed=False, required=True)
    delta = ndb.IntegerProperty(required=True)
    new_balance = ndb.IntegerProperty(indexed=False, required=True)
    old_balance = ndb.IntegerProperty(indexed=False, required=True)
    other_tx = ndb.KeyProperty(kind='WalletTransaction', required=True)
    receiver = ndb.KeyProperty(Account, required=True)
    sender = ndb.KeyProperty(Account, required=True)
    timestamp = ndb.DateTimeProperty(auto_now_add=True, required=True)

    @classmethod
    def make_hash(cls, wallet, sender_key, receiver_key, delta):
        s = '%s %d %d %d %d' % (wallet.last_tx, sender_key.id(), receiver_key.id(),
                                wallet.balance, delta)
        return hashlib.sha256(s).hexdigest()

    @property
    def receiver_wallet(self):
        assert self.delta != 0
        return self.key.parent() if self.delta > 0 else self.other_tx.parent()

    @property
    def sender_wallet(self):
        assert self.delta != 0
        return self.key.parent() if self.delta < 0 else self.other_tx.parent()
