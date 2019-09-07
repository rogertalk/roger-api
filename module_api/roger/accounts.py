# -*- coding: utf-8 -*-

from collections import defaultdict, namedtuple
from datetime import datetime
import logging
import os
import re

from google.appengine.api import taskqueue
from google.appengine.ext import ndb

from flask import has_request_context, request

from roger import auth, config, files, models, notifs, report, slack_api, streams, strings
from roger import threads
from roger_common import errors, identifiers, random, security


class HookCollection(dict):
    def trigger(self, *args, **kwargs):
        # Note: Generally the first positional argument should be an account handler.
        for hook_name, hook in self.iteritems():
            try:
                hook(*args, **kwargs)
            except:
                logging.exception('Failed run hook "%s"' % (hook_name,))


activation_hooks = HookCollection()
create_hooks = HookCollection()
_static_handlers = {}
_status_handlers = {}


def create(identifier=None, display_name=None, image=None, status='temporary', status_reason=None, **kwargs):
    _validate_status_transition(None, status)
    if status == 'voicemail' and not display_name:
        # Special rule for voicemail display phone number.
        display_name = identifier
    if image and not isinstance(image, basestring):
        image = files.upload(image.filename, image.stream, persist=True)
    # Create the account entity and get a handler wrapping it.
    account = models.Account.create(status, display_name=display_name,
                                    identifier=identifier, image_url=image,
                                    **kwargs)
    handler = get_handler(account)
    # Run all create hooks (generally used to add default streams to the user).
    create_hooks.trigger(handler)
    # Report account activation.
    if status == 'active':
        if not status_reason:
            status_reason = 'created_active'
        report.account_activated(handler.account_id, 'none', status_reason)
        logging.debug('Account activated due to %s', status_reason)
        activation_hooks.trigger(handler)
    return handler


def connect_email(handler):
    for identifier in handler.identifiers:
        identifier, identifier_type = identifiers.parse(identifier)
        if identifier_type != identifiers.EMAIL:
            continue
        _, team, user = identifiers.parse_service(identifier)
        handler.connect_service('email', team, user)
activation_hooks['connect_email'] = connect_email


def generate_username(handler):
    if handler.username:
        return
    if not handler.account.display_name_set:
        handler.generate_username()
        return
    base = identifiers.clean_username(handler.account.display_name)
    handler.generate_username(base)
activation_hooks['generate_username'] = generate_username


def get_account(identifier):
    """Utility function for getting an Account instance."""
    account = models.Account.resolve(identifier)
    if not account:
        raise errors.ResourceNotFound('That account does not exist')
    return account


def get_handler(identifier):
    """Returns an account handler for the given identifier."""
    if isinstance(identifier, AccountHandler):
        # Assume that account handlers have already been resolved before.
        return identifier
    # Parse and normalize the identifier.
    identifier_type = 'unknown'
    if isinstance(identifier, basestring):
        identifier, identifier_type = identifiers.parse(identifier)
    # Attempt to find a Roger account for the provided identifier.
    account = models.Account.resolve(identifier)
    if account:
        # First check if there is a static handler for the account.
        for identity in account.identifiers:
            cls = _static_handlers.get(identity.id())
            if cls:
                return cls.handler
        # Support custom handlers for specific statuses.
        if account.status in _status_handlers:
            return _status_handlers[account.status](account)
        # No special handling for this account.
        return AccountHandler(account)
    # Finally check if the identifier has a static handler.
    if identifier in _static_handlers:
        # Use a static handler for this identifier.
        return _static_handlers[identifier].handler
    # Give up.
    logging.info('Could not get a handler for "%s" (%s)', identifier, identifier_type)
    raise errors.ResourceNotFound('That account does not exist')


def get_or_create(*args, **kwargs):
    if not args:
        raise ValueError('Need at least one identifier')
    args = map(identifiers.clean, args)
    notify_change = kwargs.pop('notify_change', True)
    # Find the first account matching an identifier and attempt to add the remaining
    # identifiers to it. If no account was found, create a new one with the available
    # identifiers added to it. Identifiers belonging to other accounts are ignored.
    identity_keys = [ndb.Key('Identity', identifier) for identifier in args]
    account_key = None
    claimables = []
    for identifier, identity in zip(args, ndb.get_multi(identity_keys)):
        if not identity or not identity.account:
            claimables.append(identifier)
            continue
        if account_key and identity.account != account_key:
            logging.warning('%r does not belong to account %d',
                            identifier, account_key.id())
            continue
        account_key = identity.account
    # TODO: Transaction?
    if account_key:
        logging.debug('Found account %d for %r', account_key.id(), args)
        handler = get_handler(account_key)
        if not handler.image_url and 'image' in kwargs:
            logging.debug('Updating image for %d', account_key.id())
            handler.set_image(kwargs['image'])
    else:
        handler = create(claimables.pop(0), **kwargs)
        logging.debug('Created account %d for %r', handler.account_id, args)
    for identifier in claimables:
        # TODO: Decide if/when to notify on connect for additional identifiers.
        handler.add_identifier(identifier, notify_change=False, notify_connect=False)
    # Only trigger one change notif, and only for existing accounts.
    if account_key and claimables and notify_change:
        handler._notify_account_change()
    return handler


# Runs additional logic when an account receives a chunk.
def handle_stream_chunk(e):
    receiver = get_handler(e.event_account)
    sender = get_handler(e.stream.lookup_account(e.chunk.sender))
    receiver.on_new_chunk(sender, e.stream, e.chunk,
                          mute_notification=e.mute_notification)
notifs.add_handler(notifs.ON_STREAM_CHUNK, handle_stream_chunk)

def handle_new_stream(e):
    if e.sender_id == e.event_account_key.id():
        # Don't send the stream creator's greeting.
        return
    if len(e.stream.participants) != 2 or not e.event_account.greeting:
        return
    e.stream.send(e.event_account.greeting, e.event_account.greeting_duration,
                  show_for_sender=False, start_now=True)
notifs.add_handler(notifs.ON_STREAM_NEW, handle_new_stream)


def notify_others_on_activation(handler):
    @ndb.tasklet
    def callback(stream):
        events = defaultdict(list)
        for account_key in stream.visible_by:
            if account_key == handler.account.key:
                continue
            events[account_key].append({
                'participant': handler.account,
                'stream_id': stream.key.id()
            })
        account_keys = list(events)
        accounts = yield ndb.get_multi_async(account_keys)
        futures = []
        for i, (key, account) in enumerate(zip(account_keys, accounts)):
            hub = notifs.Hub(account)
            for event_data in events[key]:
                futures.append(hub.emit_async(notifs.ON_STREAM_PARTICIPANT_CHANGE, **event_data))
        yield tuple(futures)
    q = models.Stream.query(models.Stream.participants.account == handler.account.key)
    q.map(callback)
activation_hooks['notify_others_on_activation'] = notify_others_on_activation


def static_handler(identifier, **kwargs):
    """
    A decorator that registers a custom handler for the account with the specified
    identifier. If such an account doesn't exist, it will be created.
    """
    def wrap(cls):
        # Static handlers default to creating active accounts.
        kwargs.setdefault('status', 'active')
        cls.handler = StaticHandlerDescriptor(identifier, **kwargs)
        _static_handlers[identifier] = cls
        return cls
    return wrap


def status_handler(status):
    def wrap(cls):
        _status_handlers[status] = cls
        return cls
    return wrap


def unregister_static_handler(identifier):
    cls = _static_handlers.pop(identifier, None)
    if cls:
        delattr(cls, 'handler')


def _validate_status_transition(old_status, new_status):
    """Validates that a status may change from a certain value to another."""
    can_change = False if old_status is not None else True
    for tier in config.VALID_STATUS_TRANSITIONS:
        if old_status in tier:
            can_change = True
        if new_status in tier:
            if not can_change:
                raise errors.ForbiddenAction('Cannot change status from "%s" to "%s"' % (
                    old_status, new_status))
            break
    else:
        raise errors.InvalidArgument('Invalid status')


class AccountHandler(object):
    # Avoid accidentally setting unsupported attributes.
    __slots__ = ['account', 'identifier', 'notifs']

    def __eq__(self, other):
        if not isinstance(other, AccountHandler):
            return False
        return self.account == other.account

    def __getattr__(self, name):
        # By default, proxy to the underlying Account entity.
        return getattr(self.account, name)

    def __init__(self, account=None, identifier=None, **kwargs):
        if not account:
            account = get_account(identifier)
        self.account = account
        self.identifier = identifier
        self.notifs = notifs.Hub(account)

    @property
    def account_age(self):
        return datetime.utcnow() - self.created

    def add_identifier(self, identifier, notify_change=True, notify_connect=True, **kwargs):
        identifier, identifier_type = identifiers.parse(identifier)
        identity, account = models.Identity.add(identifier, self.account.key, **kwargs)
        if not identity:
            if self.has_identifier(identifier):
                # Just assume the identifier is already owned by the current account.
                return
            raise errors.AlreadyExists('That identifier is already in use')
        # Brazil can use two formats for one phone number.
        equivalent = self._get_alternate_identifier(identity.key.id())
        if equivalent:
            i, a = models.Identity.add(equivalent, self.account.key)
            if i and a:
                identity, account = i, a
            else:
                logging.warning('Failed to reserve %r (based on %r) for %d',
                                equivalent, identifier, self.account.key.id())
        # Update in-memory instance to reflect reality.
        self.account.populate(**account.to_dict())
        if identifier_type == identifiers.EMAIL and self.account.is_activated:
            # Connect the email "service".
            _, team, user = identifiers.parse_service(identifier)
            self.connect_service('email', team, user, notify=notify_connect)
        elif identifier_type == identifiers.SERVICE_ID:
            service, team, user = identifiers.parse_service(identifier)
            if service == 'fika':
                # fika.io "service" always gets connected.
                self.connect_service('fika', team, user, notify=notify_connect)
        if notify_change:
            self._notify_account_change()

    @ndb.tasklet
    def add_vote_async(self):
        account = yield models.Account.add_vote_async(self.account.key)
        if account:
            self.account.populate(**account.to_dict())
            raise ndb.Return(True)
        raise ndb.Return(False)

    def block(self, identifier):
        blocked_account = models.Account.resolve_key(identifier)
        if blocked_account == self.account.key:
            raise errors.InvalidArgument('You cannot block yourself')
        models.Account.add_block(blocked_account, self.account.key)
        f1 = models.AccountFollow.unfollow_async(self.account.key, blocked_account)
        f2 = models.AccountFollow.unfollow_async(blocked_account, self.account.key)
        stream = self.streams.get([blocked_account])
        if stream:
            stream.hide()
        ndb.Future.wait_all([f1, f2])

    def change_identifier(self, old, new, notify_connect=True, primary=False):
        new, identifier_type = identifiers.parse(new)
        if not new:
            logging.warning('%r is invalid', new)
            raise errors.InvalidArgument('That identifier is not valid')
        if old not in self.identifiers:
            raise errors.ForbiddenAction('That identifier belongs to another account')
        if old == new:
            return
        # Get the service, team, resource from the new identifier.
        try:
            service, team, resource = identifiers.parse_service(new)
            new_team = not self.is_on_team(service, team)
        except:
            service, team, resource = (None, None, None)
            new_team = True
        identity, account = models.Identity.change(
                old, new, assert_account_key=self.account.key, primary=primary)
        if not identity:
            raise errors.AlreadyExists('That identifier is already in use')
        # Update in-memory instance to reflect reality.
        if account:
            self.account.populate(**account.to_dict())
        if self.account.is_activated and service == 'email' and new_team:
            # Connect the email "service" (if the user is not already on this domain).
            self.connect_service(service, team, resource, notify=notify_connect)
        # TODO: We should also disconnect service if the old identifier was a service.
        self._notify_account_change()

    def change_status(self, status, status_reason=None):
        if self.account.status == status:
            # Don't do anything if there is no change.
            return
        # TODO: Report all status changes.
        old_status = self.account.status
        was_activated = self.account.is_activated
        _validate_status_transition(old_status, status)
        # Update the account and its related Identity entities.
        account = models.Account.set_status(self.account.key, status)
        self.account.populate(**account.to_dict())
        if self.account.is_activated and not was_activated:
            # Report account activation.
            if not status_reason:
                status_reason = 'set_active'
            report.account_activated(self.account_id, old_status, status_reason)
            # Run activation hooks.
            logging.debug('Account activated due to %s', status_reason)
            activation_hooks.trigger(self)
        self._notify_account_change()

    def connect_service(self, service, team, identifier, client=None, notify=True, team_properties={}, **kwargs):
        key = models.ServiceAuth.resolve_key((self.account.key, service, team))
        _, service_key, team_key = models.ServiceAuth.split_key(key)
        if team_key:
            # Ensure that the team exists.
            # TODO: In a transaction together with ServiceAuth?
            models.ServiceTeam.create_or_update(team_key, **team_properties)
        # Get or create the service authentication entry.
        auth = key.get()
        if not auth:
            auth = models.ServiceAuth(key=key, service=service_key, service_team=team_key)
        auth.service_identifier = identifier
        auth.populate(**kwargs)
        # Store the clients that have been used to connect to this service.
        client_key = ndb.Key('ServiceClient', client or 'api')
        if client_key not in auth.service_clients:
            auth.service_clients.append(client_key)
            new_client = True
        else:
            new_client = False
        auth.put()
        # Notify the first time a user connects with a new client.
        if new_client:
            logging.debug('New client %s for %s', client_key.id(), key.id())
            self.on_new_service(auth, client_key, notify=notify)
        self._notify_account_change()
        return auth

    def create_access_token(self, **kwargs):
        return self.create_session(**kwargs).to_access_token()

    def create_session(self, skip_activation=False, **kwargs):
        # Activate the user when they log in.
        if not (self.is_activated or skip_activation) or self.is_inactive:
            if not self.can_activate:
                raise errors.ForbiddenAction('Account may not be activated at this time')
            self.change_status('active', status_reason='logged_in')
        scopes = {auth.SCOPE_REFRESH_TOKEN}
        return auth.Session(self.account, scopes=scopes, **kwargs)

    def disconnect_service(self, service, team, resource):
        # Remove the service authentication entry.
        key = models.ServiceAuth.resolve_key((self.account.key, service, team))
        key.delete()
        identifier = models.Service.build_identifier(service, team, resource)
        logging.debug('Disconnected service %s for %d', identifier, self.account_id)
        if self.has_identifier(identifier):
            logging.warning('Account still has identifier %s', identifier)

    def generate_username(self, base=None):
        """Generates a new username based on display name."""
        identifier = None
        new_username = base or random.username_generator()
        while True:
            identifier, account = models.Identity.add(new_username, self.account.key,
                                                      primary=bool(base))
            if identifier:
                logging.debug('Successfully added username %r', new_username)
                self.account.populate(**account.to_dict())
                self._notify_account_change()
                return
            logging.debug('Could not add username %r', new_username)
            new_username = random.username_generator(base)

    def has_identifier(self, identifier):
        return identifiers.clean(identifier) in self.identifiers

    @property
    def has_password(self):
        """Checks if the account has a password associated with it."""
        auth = models.PasswordAuth.get_by_id(str(self.account_id))
        return bool(auth)

    @property
    def identifiers(self):
        return [identifier.id() for identifier in self.account.identifiers]

    def load(self):
        self.account.populate(**self.account.key.get().to_dict())

    def on_new_chunk(self, sender, stream, chunk, mute_notification=False):
        """Handler for when an account has received a chunk."""
        # When a temporary account receives something, it becomes invited.
        if self.is_temporary and not sender.is_bot:
            self.change_status('invited', status_reason='incoming_stream')
            report.invite(sender.account_id, self.account_id)

    def on_new_service(self, auth, client_key, notify=True):
        params = {
            'account_id': self.account_id,
            'client_id': client_key.id(),
            'notify': 'true' if notify else 'false',
            'service_id': auth.service.id(),
            'team_id': auth.service_team.id() if auth.service_team else '',
            'resource': auth.service_identifier,
        }
        logging.debug('Queueing job to set up %s for %d', auth.key.id(), self.account_id)
        taskqueue.add(method='GET', url='/_ah/jobs/set_up_new_service', params=params,
                      queue_name=config.SERVICE_QUEUE_NAME)

    def remove_identifier(self, identifier):
        if identifier not in self.identifiers:
            raise errors.ForbiddenAction('That identifier belongs to another account')
        if len(self.identifiers) < 2:
            raise errors.ForbiddenAction('Can not remove last identifier')
        account = models.Identity.release(identifier, assert_account_key=self.account.key)
        # Update in-memory instance to reflect reality.
        if account:
            self.account.populate(**account.to_dict())
        # Disconnect service if the identifier is a service identifier.
        identifier, identifier_type = identifiers.parse(identifier)
        if identifier_type in (identifiers.EMAIL, identifiers.SERVICE_ID):
            service, team, resource = identifiers.parse_service(identifier)
            self.disconnect_service(service, team, resource)
        self._notify_account_change()

    def send_greeting(self, account, mute_notification=True):
        if not self.greeting:
            logging.warning('Attempted to send greeting but there is no greeting')
            return
        self.streams.send(
            [account],
            self.greeting,
            duration=self.greeting_duration,
            mute_notification=mute_notification,
            reason='greeting')

    def set_display_name(self, display_name):
        if not isinstance(display_name, basestring):
            raise TypeError('Display name must be a string')
        # TODO: Validate display name more.
        display_name = display_name.strip()
        if not display_name:
            raise errors.InvalidArgument('Invalid display name')
        if display_name == self.account.display_name:
            return
        self.account.display_name = display_name
        self.account.put()
        if not self.account.primary_set:
            base = identifiers.clean_username(self.account.display_name)
            if base:
                logging.debug('User has no username, autosetting one')
                self.generate_username(base)
        self._notify_account_change()

    def set_greeting(self, payload, duration):
        if not files.is_persistent(payload):
            payload = files.make_persistent(payload)
        self.account.greeting = payload
        self.account.greeting_duration = duration
        self.account.put()

    def set_image(self, image):
        if image and not isinstance(image, basestring):
            image = files.upload(image.filename, image.stream, persist=True)
        self.account.image_url = image
        self.account.put()
        self._notify_account_change()

    def set_password(self, password):
        """Sets the password that is used to authenticate the account."""
        auth = models.PasswordAuth(id=str(self.account_id))
        auth.salt = os.urandom(32)
        auth.hash = security.salted_sha256(password, auth.salt)
        auth.put()

    def set_primary_identifier(self, identifier):
        """Moves an identifier to the top of the list of
        identifier, making it the primary one.
        """
        account = models.Account.set_primary_identifier(self.account.key, identifier)
        # Update in-memory instance to reflect reality
        if account:
            self.account.populate(**account.to_dict())
        self._notify_account_change()

    def set_username(self, new_username):
        new_username, identifier_type = identifiers.parse(new_username)
        if identifier_type != identifiers.USERNAME:
            # A user may not use this endpoint to add a phone number/e-mail.
            raise errors.InvalidArgument('A valid username must be provided')
        # Switch out the old username if it exists, otherwise just add the new one.
        old_username = self.username
        if old_username and self.account.primary_set:
            self.change_identifier(old_username, new_username, primary=True)
        else:
            self.add_identifier(new_username, primary=True)

    @property
    def streams(self):
        return streams.get_handler(self.account)

    @property
    def threads(self):
        return threads.Handler(self.account.key)

    def unblock(self, identifier):
        blocked_account = models.Account.resolve_key(identifier)
        models.Account.remove_block(blocked_account, self.account.key)

    def update_demographics(self, birthday, gender):
        changed = False
        if birthday is not None:
            birthday = models.Account.parse_birthday(birthday)
            models.Account.validate_birthday(birthday)
            if birthday != self.account.birthday:
                self.account.birthday = birthday
                changed = True
        if gender is not None:
            models.Account.validate_gender(gender)
            if gender != self.account.gender:
                self.account.gender = gender
                changed = True
        if not changed:
            return
        self.account.put()
        self._notify_account_change()

    def validate_password(self, password):
        """
        Checks if the provided password matches the password that is used to authenticate
        the account.
        """
        auth = models.PasswordAuth.get_by_id(str(self.account_id))
        if not auth or auth.hash != security.salted_sha256(password, auth.salt):
            return False
        return True

    def _get_alternate_identifier(self, identifier):
        # For Brazil, support multiple valid formats for one number.
        if not identifier.startswith('+55'):
            return
        if len(identifier) < 13 or identifier[-8] not in '6789':
            # Only legacy numbers have this special logic.
            return
        if len(identifier) == 13:
            # Legacy format -> new format.
            return identifier[:5] + '9' + identifier[5:]
        if len(identifier) == 14:
            # New format -> legacy format.
            return identifier[:5] + identifier[6:]

    def _notify_account_change(self):
        self.notifs.emit(notifs.ON_ACCOUNT_CHANGE, account=self.account,
                         public_options={'include_extras': True,
                                         'view_account': self.account})


class Resolver(object):
    """Helper class for dealing with destinations of a stream."""
    Route = namedtuple('Route', 'type value label')

    def __init__(self, routes=None):
        self.routes = routes or []

    def __repr__(self):
        return 'Resolver(%r)' % (self.routes,)

    def add_route(self, route_type, route, label=None):
        route = self.Route(route_type, route, label)
        score = self.route_rank(route)
        for index, other_route in enumerate(self.routes):
            if self.route_rank(other_route) < score:
                self.routes.insert(index, route)
                break
        else:
            self.routes.append(route)

    def get_or_create_account_key(self, create_status='temporary', origin_account=None):
        # Identifier types that may have an account created.
        creatable_types = (identifiers.EMAIL, identifiers.PHONE, identifiers.SERVICE_ID)
        # Try to match the destination to an existing account.
        best_route = None
        for route in self.routes:
            key = models.Account.resolve_key(route.value)
            if key:
                return key
            if route.type in creatable_types and not best_route:
                best_route = route
        # Account not found, create one based on first usable contact detail.
        # TODO: This should check properly if route is externally verifiable (e.g., SMS).
        if not best_route:
            logging.warning('Failed to create an account for one of %s', self.routes)
            return None
        identifier = best_route.value
        # Locally verified accounts can be created immediately.
        if best_route.type != identifiers.SERVICE_ID:
            handler = create(identifier, status=create_status)
            return handler.account.key
        # Verify that this user is on the same service/team as the origin account.
        if not origin_account:
            raise errors.InvalidArgument('Cannot use third-party accounts')
        service_key, team_key, resource = models.Service.parse_identifier(identifier)
        if not origin_account.is_on_team(service_key, team_key):
            raise errors.InvalidArgument('Invalid third-party account')
        # Look up the third-party account.
        # TODO: Support multiple types of services dynamically.
        if service_key.id() != 'slack':
            raise errors.NotSupported('Only Slack accounts are supported')
        auth = origin_account.get_auth_key(service_key, team_key).get()
        # TODO: Put this API call elsewhere!
        info = slack_api.get_user_info(resource, auth.access_token)
        ids = [identifier, info['user']['profile']['email']]
        handler = get_or_create(
            *ids, display_name=info['user']['real_name'] or info['user']['name'],
            image=info['user']['profile'].get('image_original'), status=create_status)
        return handler.account.key

    @classmethod
    def parse(cls, value):
        if not isinstance(value, basestring):
            raise TypeError('Destination value must be a string')
        destination = cls()
        # Value is a comma separated list of routes.
        for route in value.split(','):
            # Route can contain a label prefix (with a colon to separate it).
            label_and_route = route.split(':', 1)
            if len(label_and_route) == 1:
                label = None
                route = label_and_route[0]
            else:
                label = re.sub(r'[\W_]', '', label_and_route[0].lower())
                route = label_and_route[1]
            # Clean up route and get its type.
            route, route_type = identifiers.parse(route)
            destination.add_route(route_type, route, label)
        return destination

    @property
    def primary_route(self):
        return self.routes[0]

    @staticmethod
    def route_rank(route):
        # Score the route based on certain values.
        score = 0
        if route.type in (identifiers.ACCOUNT_ID, identifiers.USERNAME):
            score += 500
        elif route.type == identifiers.SERVICE_ID:
            score += 400
        elif route.type == identifiers.EMAIL:
            score += 10
        elif route.type == identifiers.PHONE:
            # TODO: Use Twilio API to look up numbers?
            if route.value.startswith('+3519') or route.value.startswith('+467'):
                # Swedish and Spanish cellular phone numbers are predictable.
                score += 5
            score += 100
        else:
            logging.debug('Unhandled route type: %s', route.type)
        if route.label == 'backend':
            score += 75
        elif route.label == 'iphone':
            score += 55
        elif route.label == 'mobile':
            score += 50
        elif route.label == 'main':
            score += 10
        elif route.label in ('home', 'work'):
            # These are unlikely to be cell phones, so penalize their score.
            score -= 10
        elif route.label:
            logging.debug('Unhandled label: %s', route.label)
        return score


class StaticHandlerDescriptor(object):
    """
    A lazy evaluator of a single instance for the specified identifier. The type of the
    instance will be the type that this descriptor is assigned to.

    Important: The account will be created or loaded immediately when this descriptor is
    created.
    """
    def __init__(self, identifier, **kwargs):
        self.account = models.Account.resolve(identifier)
        if not self.account:
            self.account = create(identifier, **kwargs).account
        self.identifier = identifier
        self._instance = None

    def __get__(self, obj, cls):
        if not self._instance:
            self._instance = cls(account=self.account, identifier=self.identifier)
        return self._instance
