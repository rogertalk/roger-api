# -*- coding: utf-8 -*-

import collections
from datetime import date, datetime, timedelta
from functools import wraps
import logging
import os
import time

from google.appengine.ext import ndb

from flask import g, has_request_context, request

from roger import config, external, localize, models, proto, services, slack_api
from roger_common import convert, errors, events, flask_extras
from roger_common import identifiers, random, security


_scopes = dict()
Scope = collections.namedtuple('Scope', ['value', 'description'])


def create_scope(bit, description):
    if not isinstance(bit, (int, long)) or not isinstance(description, basestring):
        raise TypeError('A scope must be created with a bit and a description')
    if bit < 0:
        raise ValueError('Bit must not be negative')
    bit_value = 1 << bit
    if bit_value in _scopes:
        raise ValueError('Scope bit is already in use')
    scope = Scope(bit_value, description)
    _scopes[bit_value] = scope
    return scope


SCOPE_REFRESH_TOKEN = create_scope(0, 'issue a refresh token')


def bits_to_scopes(value):
    scopes = set()
    for bit in _scopes:
        if not value & bit:
            continue
        value -= bit
        scopes.add(_scopes[bit])
    if value:
        raise ValueError('Unsupported scope(s)')
    return scopes


def get_scopes(value_or_scopes):
    if not value_or_scopes:
        return set()
    if isinstance(value_or_scopes, (int, long)):
        return bits_to_scopes(value_or_scopes)
    scopes = set(value_or_scopes)
    for scope in scopes:
        if not isinstance(scope, Scope):
            raise TypeError('Scope list contained an invalid value')
    return scopes


def scopes_to_bits(scopes):
    value = 0
    for scope in scopes:
        if not isinstance(scope, Scope):
            raise TypeError('Not a Scope instance')
        if scope.value not in _scopes:
            raise ValueError('Unsupported scope(s)')
        value |= scope.value
    return value


_challengers = dict()


class Session(object):
    def __init__(self, account, access_token=None, created=None, ttl=None, scopes=None, extra_data={}):
        self._access_token = access_token
        if isinstance(account, models.Account):
            self._account = account
            self.account_key = account.key
        else:
            self._account = None
            self.account_key = models.Account.resolve_key(account)
            if not self.account_key:
                raise ValueError('Expected a valid Account key or instance')
        if not isinstance(extra_data, dict):
            raise TypeError('extra_data must be a dict')
        self.created = created or datetime.utcnow()
        self.ttl = ttl or 3600
        self.scopes = get_scopes(scopes)
        self.extra_data = extra_data.copy()

    @property
    def account(self):
        if not self._account:
            self._account = self.account_key.get()
        return self._account

    @property
    def account_id(self):
        return self.account_key.id()

    def create_auth_code(self, client_id, redirect_uri=None):
        secret = random.base62(config.AUTH_CODE_LENGTH)
        expires = datetime.utcnow() + config.AUTH_CODE_TTL
        client = services.get_client(client_id)
        if redirect_uri:
            if redirect_uri not in client.redirect_uris:
                raise errors.InvalidArgument('Invalid redirect_uri value')
        elif len(client.redirect_uris) == 1:
            # Figure out the redirect URI from the client.
            redirect_uri = client.redirect_uris[0]
        if not redirect_uri:
            # Ensure that it's not the empty string.
            redirect_uri = None
        code = models.AuthCode(id=secret, account=self.account.key, expires=expires,
                               client_id=client_id, redirect_uri=redirect_uri)
        code.put()
        return code

    def create_refresh_token(self):
        if not self.has_scope(SCOPE_REFRESH_TOKEN):
            return None
        data = proto.RefreshToken(
            account_id=self.account_id,
            scopes=scopes_to_bits(self.scopes))
        binary = data.SerializeToString()
        return security.encrypt(config.ENCRYPTION_KEY, binary)

    @property
    def expires(self):
        return self.created + timedelta(seconds=self.ttl)

    @classmethod
    def from_access_token(cls, token):
        data = proto.AccessToken()
        try:
            data.ParseFromString(security.decrypt(config.ENCRYPTION_KEY, token))
        except:
            raise errors.InvalidAccessToken()
        return cls(data.account_id,
                   access_token=token,
                   created=datetime.fromtimestamp(data.created),
                   ttl=data.ttl,
                   scopes=data.scopes)

    @classmethod
    def from_auth_code(cls, secret, client_id, redirect_uri=None):
        code = models.AuthCode.get_by_id(secret)
        if not code or code.expires <= datetime.utcnow():
            raise errors.InvalidCredentials()
        if client_id != code.client_id or redirect_uri != code.redirect_uri:
            raise errors.InvalidCredentials()
        code.key.delete()
        return cls(code.account)

    @classmethod
    def from_refresh_token(cls, token):
        data = proto.RefreshToken()
        try:
            data.ParseFromString(security.decrypt(config.ENCRYPTION_KEY, token))
        except:
            raise errors.InvalidAccessToken()
        return cls(data.account_id, scopes=data.scopes)

    def has_scopes(self, *args):
        return self.scopes.issuperset(args)
    has_scope = has_scopes

    def public(self, **kwargs):
        delta = self.expires - datetime.utcnow()
        data = dict(
            access_token=self.to_access_token(),
            token_type='bearer',
            status=self.account.status,
            expires_in=int(round(delta.total_seconds())),
            account=self.account,
            **self.extra_data)
        refresh_token = self.create_refresh_token()
        if refresh_token:
            data['refresh_token'] = refresh_token
        return data

    def to_access_token(self):
        # OAuth2 data which will be encrypted as the access token.
        data = proto.AccessToken(
            account_id=self.account_id,
            scopes=scopes_to_bits(self.scopes),
            created=convert.unix_timestamp(self.created),
            ttl=self.ttl)
        binary = data.SerializeToString()
        return security.encrypt(config.ENCRYPTION_KEY, binary)


def authed_request(allow_nonactive=False, set_view_account=False, update_last_active=True):
    def decorator(func):
        @wraps(func)
        def wrap(*args, **kwargs):
            time_1 = time.clock()
            session = get_session()
            if not session or not session.account:
                raise errors.InvalidAccessToken()
            # Ensure that the account is active.
            if session.account.can_make_requests:
                if update_last_active:
                    # Update last active date if they haven't been active earlier today.
                    today = date.today()
                    if session.account.last_active < today:
                        session.account.last_active = today
                        agent = request.headers.get('User-Agent')
                        if agent:
                            session.account.last_active_client = agent
                        session.account.put()
            elif not allow_nonactive:
                logging.warning('Account %d is %s', session.account_id,
                                session.account.status)
                if session.account.is_activated:
                    # The account has been activated but can't make requests so it's disabled.
                    raise errors.AccountDisabled()
                else:
                    raise errors.ForbiddenAction('Account is not active')
            # Update the user's location coordinates if provided.
            client_id, _ = get_client_details()
            if session.account.share_location and config.LOCATION_HEADER in request.headers:
                try:
                    latlng = request.headers[config.LOCATION_HEADER]
                    point = ndb.GeoPt(latlng)
                    session.account.set_location(point)
                except:
                    pass
            elif client_id in ('fika', 'reactioncam') and 'X-AppEngine-CityLatLong' in request.headers:
                latlng = request.headers['X-AppEngine-CityLatLong']
                session.account.set_location(ndb.GeoPt(latlng), timezone_only=True)
            if set_view_account:
                g.public_options['view_account'] = session.account
            time_2 = time.clock()
            try:
                result = func(session, *args, **kwargs)
            finally:
                time_3 = time.clock()
                logging.debug('Account %d', session.account_id)
            return result
        return wrap
    return decorator


def challenger(identifier_type):
    def wrap(cls):
        _challengers[identifier_type] = cls
        return cls
    return wrap


def get_challenger(client, identifier, call=False):
    """
    Returns a handler for creating a challenge for the given identifier.
    """
    identifier, identifier_type = identifiers.parse(identifier)
    if identifier in config.DEMO_ACCOUNTS:
        # Demo accounts.
        return DummyChallenger(client, identifier)
    if identifier_type == identifiers.PHONE and call:
        return CallChallenger(client, identifier)
    challenger = _challengers.get(identifier_type)
    if not challenger:
        logging.warning('Failed to get a challenger for %r', identifier)
        raise errors.InvalidArgument('That identifier is not valid')
    return challenger(client, identifier)


def get_client_details():
    """Extracts the client id and secret from the HTTP request."""
    if not has_request_context():
        return None, ''
    basic = request.authorization
    if basic:
        return basic.username, basic.password
    else:
        client_id = flask_extras.get_parameter('client_id')
        client_secret = flask_extras.get_parameter('client_secret') or ''
    if not client_id:
        agent = request.headers.get('User-Agent', '')
        if 'RogerAndroid/' in agent:
            client_id = 'android'
        elif 'Roger/' in agent:
            client_id = 'ios'
        elif 'Fika/' in agent:
            client_id = 'fika'
        elif 'FikaDesktop/' in agent:
            # TODO: Unique client id?
            client_id = 'fika'
        elif 'FikaIO/' in agent:
            client_id = 'fikaio'
        elif 'ReactionCam/' in agent:
            client_id = 'reactioncam'
        elif 'roger-web-client' in agent or 'Mozilla' in agent or 'Opera' in agent:
            client_id = 'web'
    return client_id, client_secret


def get_session():
    try:
        access_token = request.args.get('access_token')
        if not access_token:
            authorization = request.headers['Authorization']
            token_type, access_token = authorization.split(' ')
            assert token_type == 'Bearer'
        session = Session.from_access_token(access_token)
    except:
        return None
    # Allow admins to override the session account id.
    # TODO: This needs to be checked on the token, so that a token for an
    #       admin granted to a third-party app can't also do this.
    on_behalf_of = request.args.get('on_behalf_of')
    if on_behalf_of:
        if session.account.admin:
            session = Session(int(on_behalf_of))
        else:
            raise errors.ForbiddenAction('Forbidden use of on_behalf_of')
    return session


class Challenger(object):
    def __init__(self, client, identifier):
        self.client = client
        self.identifier = identifiers.clean(identifier)
        if not self.identifier:
            logging.warning('Cleaning %r resulted in empty value', identifier)
            raise errors.InvalidArgument('That identifier is not valid')
        # Team will be set to preferred team after challenge is complete.
        self.team = None

    def _deliver(self, code):
        raise NotImplementedError()

    def challenge(self):
        if self.client == 'fika':
            # TODO: Support more than email.
            try:
                service_key, team_key, _ = models.Service.parse_identifier(self.identifier)
            except ValueError:
                raise ValueError('%r could not be used as email' % (self.identifier,))
            if team_key.id() in config.PUBLIC_EMAIL_DOMAINS:
                # Explicitly ignore public email domains.
                team = None
            else:
                # Ensure that the team exists and fetch it.
                team = models.ServiceTeam.create_or_update(team_key)
            login_enabled = (team and team.whitelisted)
            block_due_to_roger = False
            event = events.FikaLoginV3(blocked=False, identifier=self.identifier)
            if not login_enabled:
                # Check the account for another valid team which will let them in.
                # TODO: Improve this logic.
                account = models.Account.resolve(self.identifier)
                event.account_id = account.key.id() if account else None
                event.has_roger = account.has_roger if account else None
                if account and account.can_activate and g.api_version >= 34:
                    if not account.has_roger:
                        logging.debug('Allowing login for %s (id: %d, status: %r)',
                                      self.identifier, account.key.id(), account.status)
                        login_enabled = True
                    else:
                        block_due_to_roger = True
                        logging.debug('Not allowing login for Roger account %s (%d)',
                                      self.identifier, account.key.id())
                elif account:
                    # If we get here, it means the account status is "waiting".
                    logging.debug('%s (%d) is not whitelisted, checking other teams',
                                  self.identifier, account.key.id())
                    for id_key in account.identifiers:
                        try:
                            s, t, _ = models.Service.parse_identifier(id_key.id())
                            if s.id() in ('fika', 'slack'):
                                team = t.get()
                                login_enabled = True
                                logging.debug('Allowing login because of %s', id_key.id())
                                break
                            elif s.id() == 'email' and t != team_key:
                                # Only emails use whitelisting.
                                team = t.get()
                                login_enabled = t.get().whitelisted
                                if login_enabled:
                                    logging.debug('Allowing login because of another whitelisted email')
                                    break
                            logging.debug('%s is not whitelisted', id_key.id())
                        except:
                            logging.debug('Skipping %s', id_key.id())
                            continue
            # Enforce block list.
            if identifiers.email(self.identifier) in config.BLOCKED_EMAILS:
                login_enabled = False
            # Block any users who are not whitelisted somehow.
            if not login_enabled:
                logging.warning('Blocking %s (not on a valid team)', self.identifier)
                if block_due_to_roger:
                    # We can assume the account variable is set here.
                    link = 'https://api.rogertalk.com/admin/accounts/%d/' % (account.key.id(),)
                    template = 'Blocked Roger user (*{}*): {{}}'.format(
                        '<%s|%s>' % (link, account.display_name))
                else:
                    template = 'Blocked login: {}'
                if identifiers.email(self.identifier) not in config.MUTED_EMAILS:
                    # TODO: Don't assume self.identifier is an email.
                    slack_api.message(channel='#review', hook_id='fika',
                                      text=template.format(identifiers.email(self.identifier)))
                    event.blocked = True
                    event.report()
                raise errors.ForbiddenAction('That identifier has not been enabled for login')
            event.report()
            self.team = team
        challenge = models.Challenge.get(self.client, self.identifier, self.code_length)
        self._deliver(challenge.code)

    @property
    def code_length(self):
        return config.CHALLENGE_CODE_LENGTH

    def validate(self, secret):
        result = models.Challenge.validate(self.client, self.identifier, secret)
        if result == models.Challenge.SUCCESS:
            return True
        elif result == models.Challenge.INVALID_SECRET:
            raise errors.InvalidArgument('An invalid secret was provided')
        elif result == models.Challenge.TOO_MANY_ATTEMPTS:
            raise errors.ResourceNotFound('Too many attempts')
        elif result == models.Challenge.EXPIRED:
            raise errors.ResourceNotFound('Challenge has expired')
        # Unexpected result.
        raise errors.ServerError()


class CallChallenger(Challenger):
    method = 'call_code'

    def _deliver(self, code):
        external.phone_call(self.identifier, 'challenge_code', args=dict(code=code))


class DummyChallenger(Challenger):
    """This challenger can be used for testing purposes and App Store review."""
    method = 'dummy'

    def challenge(self):
        # This challenger does nothing.
        pass

    def validate(self, secret):
        # The code 123/1234/12345/etc (depending on code length) always works.
        return secret == '123456789'[:self.code_length]


@challenger(identifiers.EMAIL)
class EmailChallenger(Challenger):
    method = 'email_code'

    def _deliver(self, code, use_pdf=False):
        to = identifiers.email(self.identifier)
        if not use_pdf:
            localize.send_email(self.client, 'login', to=to, code=code)
            return
        with open('strings/fikaintro.pdf') as fh:
            pdf = fh.read()
        attachments = [('%s.pdf' % (code,), pdf)]
        localize.send_email(self.client, 'login_pdf', to=to, attachments=attachments, code=code)


@challenger(identifiers.PHONE)
class SMSChallenger(Challenger):
    method = 'sms_code'

    def _deliver(self, code):
        external.send_sms(self.identifier, 'challenge_code', args=dict(code=code))
