from datetime import datetime, timedelta
import json

import mock

from roger import accounts, models
import rogertests


class BaseTestCase(rogertests.JSONServiceTestCase):
    def token(self, grant_type, client_id='test_client', version=30, **kwargs):
        params = dict(grant_type=grant_type, client_id=client_id)
        params.update(kwargs)
        return self.post('/oauth2/token?api_version=%d' % (version,), **params)

    def setUp(self):
        super(BaseTestCase, self).setUp()

        # Set up a test client for the OAuth app.
        from roger.apps import oauth2
        self.client = oauth2.app.test_client()

        # Make sure the bots are initialized during this test.
        import roger.bots
        reload(roger.bots)

        client = models.ServiceClient(
            id='test_client',
            client_secret='test_secret',
            title='Test Client')
        client.put()

        # Set up an account to test logging in.
        self.anna = accounts.create('anna')
        self.anna.set_password('pa$$w0rd')
        self.ricardo = accounts.create('ricardo')
        with mock.patch('roger.files.gcs'):
            self.ricardo.set_greeting('test.mp3', 1234)


class AuthCode(BaseTestCase):
    def test_expired_auth_code(self):
        with mock.patch('roger.auth.datetime') as datetime_mock:
            datetime_mock.utcnow.return_value = datetime.utcnow() - timedelta(minutes=99)
            code = self.anna.create_session().create_auth_code('test_client').key.id()
        result, status = self.token('authorization_code', code=code)
        self.assertValidResult(result, status, 401)

    def test_invalid_auth_code(self):
        result, status = self.token('authorization_code', code='$boo$')
        self.assertValidResult(result, status, 401)

    def test_valid_auth_code(self):
        code = self.anna.create_session().create_auth_code('test_client').key.id()
        result, status = self.token('authorization_code', code=code)
        self.assertValidResult(result, status, 200)
        self.assertIsInstance(result.get('access_token'), basestring)
        self.assertIsAccount(result.get('account'), self.anna, version=30)


class General(BaseTestCase):
    def test_missing_client_id(self):
        result, status = self.token('password', client_id=None,
                                    username='anna',
                                    password='pa$$w0rd')
        self.assertValidResult(result, status, 400)

    def test_unknown_client_id(self):
        result, status = self.token('password', client_id='commodore64',
                                    username='anna',
                                    password='pa$$w0rd')
        self.assertValidResult(result, status, 400)

    def test_unsupported_grant_type(self):
        # Ensure that an unsupported grant type gives "Not Implemented".
        result, status = self.token('random', username='anna', password='pa$$w0rd')
        self.assertValidResult(result, status, 501)


class Password(BaseTestCase):
    def log_in(self, username, password, **kwargs):
        return self.token('password', username=username, password=password, **kwargs)

    def test_incorrect_password(self):
        # Ensure that an incorrect password gives "Unauthorized".
        result, status = self.log_in('anna', 'st0p')
        self.assertValidResult(result, status, 401)

    def test_non_existent_user(self):
        # Ensure that a non-existent user gives "Unauthorized".
        result, status = self.log_in('bob', 'pa$$w0rd')
        self.assertValidResult(result, status, 401)

    def test_valid_credentials(self):
        # Ensure that logging in with valid credentials works.
        result, status = self.log_in('anna', 'pa$$w0rd')
        self.assertValidResult(result, status, 200)
        self.assertIsInstance(result.get('access_token'), basestring)
        self.assertIsAccount(result.get('account'), self.anna, version=30)


class RefreshToken(BaseTestCase):
    def test_valid_refresh_token(self):
        token = self.anna.create_session().create_refresh_token()
        self.assertIsInstance(token, basestring, 'Could not generate refresh token')
        result, status = self.token('refresh_token', refresh_token=token, version=30)
        self.assertValidResult(result, status, 200)
        self.assertIsInstance(result.get('access_token'), basestring)
        self.assertIsAccount(result.get('account'), self.anna, version=30)
        # Ensure that settings propagated to the session data.
        self.assertIn('share_location', result['account'])
