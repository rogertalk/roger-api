# -*- coding: utf-8 -*-

import base64
from datetime import datetime, timedelta
import json
from StringIO import StringIO
import time
import unittest
from urlparse import urlparse

from google.appengine.ext import ndb

import mock
from mock import ANY, call

from roger import accounts, config, files, location, models
from roger_common import convert, errors, identifiers, reporting
import rogertests


def to_dict(value, **kwargs):
    return json.loads(convert.to_json(value, **kwargs))


class BaseTestCase(rogertests.JSONServiceTestCase):
    chunks_sent = 0

    def assertIsValidSession(self, result, handler=None, expected_status=None, version=30):
        self.assertIsInstance(result.get('access_token'), basestring)
        self.assertEqual(result.get('status'), expected_status)
        self.assertEqual(result.get('token_type'), 'bearer')
        self.assertIsInstance(result.get('expires_in'), (int, long))

        self.assertIsInstance(result.get('account'), dict)
        if handler:
            self.assertIsAccount(result['account'], handler, version)
        else:
            self.assertIsInstance(result['account'].get('id'), (int, long))
            identifiers = result['account'].get('identifiers')
            self.assertIsInstance(identifiers, list)

    @mock.patch('roger.auth.EmailChallenger._deliver')
    @mock.patch('roger.auth.SMSChallenger._deliver')
    @mock.patch('roger.auth.CallChallenger._deliver')
    @mock.patch('roger.apps.api.memcache')
    def request_code(self, identifier, memcache_mock, call_deliver_mock, sms_deliver_mock, email_deliver_mock, **kwargs):
        # deactivate throttler
        memcache_mock.get.return_value = False
        result, status = self.post('/v30/challenge', identifier=identifier, **kwargs)
        self.assertValidResult(result, status, 200)
        if sms_deliver_mock.called:
            return sms_deliver_mock.call_args[0][0]
        if call_deliver_mock.called:
            return call_deliver_mock.call_args[0][0]
        elif email_deliver_mock.called:
            return email_deliver_mock.call_args[0][0]

    @mock.patch('roger.files.gcs')
    def send_chunk(self, sender, participants_or_stream_id, gcs_mock, **kwargs):
        version = kwargs.pop('version', 30)
        payload = StringIO(kwargs.pop('payload', str(BaseTestCase.chunks_sent)))
        params = {'access_token': sender.create_access_token(),
                  'payload': (payload, 'filename.mp3'),
                  'duration': 1234}
        BaseTestCase.chunks_sent += 1
        if isinstance(participants_or_stream_id, (int, long)):
            path = '/v%d/streams/%d/chunks' % (version, participants_or_stream_id)
        else:
            path = '/v%d/streams' % version
            participants = participants_or_stream_id
            if not isinstance(participants, list):
                participants = [participants]
            params['participant'] = participants
        params.update(kwargs)
        return self.post(path, **params)

    def setUp(self):
        super(BaseTestCase, self).setUp()

        # Set up a test client for the API.
        from roger.apps import api
        self.client = api.app.test_client()

        # Make sure the bots are initialized during this test.
        import roger.bots
        reload(roger.bots)

        # Create the services and teams used in tests.
        models.Service(id='email', title='Email').put()
        _, team_key = models.Service.parse_team('email:g.co/*')
        models.ServiceTeam.create_or_update(team_key)
        _, team_key = models.Service.parse_team('email:example.com/*')
        models.ServiceTeam.create_or_update(team_key)
        models.Service(id='ikea', title='IKEA').put()
        _, team_key = models.Service.parse_team('ikea:team1/*')
        models.ServiceTeam.create_or_update(team_key)
        models.Service(id='youtube', title='YouTube').put()

        # Set up a few accounts to use for testing.
        self.anna = accounts.create('anna', quality_=3, status='active')
        self.anna.add_identifier('ikea:team1/anna')
        self.anna.connect_service('ikea', 'team1', 'anna', notify=False)
        self.bob = accounts.create('bob', status='active')
        self.cecilia = accounts.create('cecilia', status='active')
        models.Device.add(self.cecilia.account,
                          app='cam.reaction.ReactionCam',
                          device_id='123456abcdef',
                          device_info='ReactionCam/123 Test/1',
                          environment='development',
                          platform='ios',
                          token='adbecf142536',
                          api_version=50)
        self.dennis = accounts.create('dennis', status='active')
        models.Device.add(self.dennis.account,
                          app='cam.reaction.ReactionCam',
                          device_id='abcdef123456',
                          device_info='ReactionCam/123 Test/1',
                          environment='development',
                          platform='ios',
                          token='defabc456123',
                          api_version=51)
        self.ricardo = accounts.create('ricardo', status='active')
        with mock.patch('roger.files.gcs') as gcs_mock:
            self.ricardo.set_greeting('test.mp3', 1234)

        # Create the Anonymous account for originals.
        anonymous_key = ndb.Key('Account', config.ANONYMOUS_ID)
        identity = models.Identity.claim('anonymous')
        identity.account = anonymous_key
        identity.status = 'active'
        identity.put()
        anonymous = models.Account(key=identity.account,
            identifiers=[identity.key],
            primary_set=True,
            properties={},
            status=identity.status,
            stored_display_name='Anonymous')
        anonymous.put()

        # Create the @reaction.cam account.
        rcam_key = ndb.Key('Account', config.REACTION_CAM_ID)
        identity = models.Identity.claim('reaction.cam')
        identity.account = rcam_key
        identity.status = 'active'
        identity.put()
        rcam = models.Account(key=identity.account,
            identifiers=[identity.key],
            primary_set=True,
            properties={},
            status=identity.status,
            verified=True)
        rcam.put()

        # Create a wallet for some of the accounts.
        self.anna.account, _ = models.Wallet.create(self.anna.key)
        self.bob.account, _ = models.Wallet.create(self.bob.key)
        models.Wallet.create(rcam_key)

        # TODO: We used to mock api.report here to measure it. This does not work when
        #       code outside api reports something so it's prone to error. We need to
        #       instead create a new reporter that counts calls to report and inject that
        #       here. Luckily, the reporting API supports this. See test_report.py for an
        #       example. We should expand on that class and use it here as well.

        patcher = mock.patch('roger.localize.send_email')
        self.addCleanup(patcher.stop)
        self.send_email_mock = patcher.start()

        patcher = mock.patch('roger.files._s3_upload')
        self.addCleanup(patcher.stop)
        self.files_s3_upload_mock = patcher.start()
        self.files_s3_upload_mock.return_value = 'https://example.com/test.xyz'

        patcher = mock.patch('roger.external.send_sms')
        self.addCleanup(patcher.stop)
        self.send_sms_mock = patcher.start()

        patcher = mock.patch('roger.external.phone_call')
        self.addCleanup(patcher.stop)
        self.phone_call_mock = patcher.start()

        patcher = mock.patch('roger.location.LocationInfo.from_point')
        self.addCleanup(patcher.stop)
        self.location_from_point_mock = patcher.start()
        # Set up a default location result.
        self.location_from_point_mock.return_value = location.LocationInfo(
            city='New York',
            country='United States',
            location=ndb.GeoPt(40.722, -73.994),
            timezone='America/New_York')


class AccountActivation(BaseTestCase):
    def test_activation_of_new_user(self):
        identifier = '+2 345-678-9012'
        # Make sure the destination user doesn't exist yet.
        with self.assertRaises(errors.ResourceNotFound):
            accounts.get_handler(identifier)
        # Send a message from an active account.
        sender = accounts.create('bobby', status='active')
        self.send_chunk(sender, identifier)
        # Make sure receiver account now exists and is invited.
        user = accounts.get_handler(identifier)
        self.assertEqual(user.account.status, 'invited')
        # Ensure that the chunk is in receiver's account.
        stream = user.streams.get([sender.account])
        self.assertTrue(stream.has_chunks)
        self.assertEqual(stream.chunks[-1].sender, sender.account.key)

    def test_becomes_invited_after_communication(self):
        identifier = '+1 234-567-8901'
        # Ensure that a temporary user becomes invited after receiving a message.
        user = accounts.create(identifier)
        self.assertEqual(user.account.status, 'temporary')
        self.send_chunk(accounts.create('alice', status='active'), identifier)
        # It should now be active in the datastore.
        self.assertEqual(accounts.get_handler(identifier).account.status, 'invited')


class Challenge(BaseTestCase):
    def test_add_identifier_to_account(self):
        # Attempt to add a phone number to an account that already exists.
        identifier = '+15551234567'
        # Make sure the identifier is not on the account already.
        self.assertNotIn(identifier, self.anna.identifiers)
        # Add the identifier to the account with a challenge/response.
        token = self.anna.create_access_token()
        code = self.request_code(identifier, access_token=token)
        result, status = self.post('/v30/challenge/respond', identifier=identifier,
                                   secret=code, access_token=token)
        self.assertValidResult(result, status, 200)
        # Validate the data in the datastore.
        self.assertIn(identifier, accounts.get_handler('anna').identifiers)

    def test_set_username_twice_works(self):
        self.anna.load()
        self.assertIn('anna', self.anna.identifiers)
        before_length = len(self.anna.identifiers)
        # Set username to "anna" again.
        result, status = self.post('/v30/profile/me', username='anna',
                                   access_token=self.anna.create_access_token())
        self.assertValidResult(result, status, 200)
        self.assertEqual(len(result['identifiers']), before_length)

    def test_backdoor(self):
        # Ensure that the backdoor logic is working.
        identifier = '+1-800-555-0199'
        code = self.request_code(identifier)
        result, status = self.post('/v30/challenge/respond', identifier=identifier,
                                   secret=code, backdoor='letmein')
        self.assertValidResult(result, status, 200)
        self.assertEqual(result.get('status'), 'active')
        # TODO: Ensure that the relevant events were reported.
        #self.assertEquals(1, self.report_mock.user_logged_in.call_count)
        #self.assertEquals(1, self.report_mock.user_registered.call_count)

    def test_challenge_email(self):
        # Ensure that response is "email_code" if email identifier provided.
        result, status = self.post('/v30/challenge', identifier='dude@company.com')
        self.assertValidResult(result, status, 200)
        self.assertIn('challenge', result)
        self.assertEqual(result['challenge'], 'email_code')
        self.assertTrue(self.send_email_mock.called)
        self.assertEquals('dude@company.com', self.send_email_mock.call_args[1]['to'])
        # TODO: Ensure that the relevant events were reported.
        #self.assertEquals(1, self.report_mock.challenge_request.call_count)

    def test_challenge_invalid_identifier(self):
        # Ensure that response is 400 if username provided.
        result, status = self.post('/v30/challenge', identifier='blixt')
        self.assertValidResult(result, status, 400)
        # weird identifier
        result, status = self.post('/v30/challenge', identifier='!@')
        self.assertValidResult(result, status, 400)
        # empty
        result, status = self.post('/v30/challenge', identifier='')
        self.assertValidResult(result, status, 400)
        self.assertFalse(self.send_sms_mock.called)
        self.assertFalse(self.send_email_mock.called)
        # TODO: Ensure that nothing was reported.
        #self.assertEquals(0, self.report_mock.challenge_request.call_count)

    def test_challenge_phone(self):
        # Ensure that response is "sms_code" if phone identifier provided.
        result, status = self.post('/v30/challenge', identifier='+13470004321')
        self.assertValidResult(result, status, 200)
        self.assertIn('challenge', result)
        self.assertEqual(result['challenge'], 'sms_code')
        self.assertTrue(self.send_sms_mock.called)
        # TODO: Ensure that the relevant events were reported.
        #self.assertEquals(1, self.report_mock.challenge_request.call_count)

    def test_challenge_phone_call(self):
        # Ensure that response is "sms_code" if phone identifier provided.
        result, status = self.post('/v30/challenge', call='true', identifier='+13479314605')
        self.assertValidResult(result, status, 200)
        self.assertIn('challenge', result)
        self.assertEqual(result['challenge'], 'call_code')
        self.assertTrue(self.phone_call_mock.called)

    def test_challenge_respond_create_account(self):
        # Verify with a new account.
        identifier = '+10001114444'
        code1 = self.request_code(identifier)
        result, status = self.post('/v30/challenge/respond', identifier=identifier,
                                   secret=code1)
        self.assertValidResult(result, status, 200)
        account_id1 = result['account']['id']
        # TODO: Ensure that the relevant events were reported.
        #self.assertEquals(1, self.report_mock.user_logged_in.call_count)
        #self.assertEquals(1, self.report_mock.user_registered.call_count)

        # Verify with the same account.
        code2 = self.request_code(identifier)
        result, status = self.post('/v30/challenge/respond', identifier=identifier,
                                   secret=code2)
        self.assertValidResult(result, status, 200)
        self.assertIn('account', result)
        account_id2 = result['account']['id']
        self.assertNotEqual(code1, code2)
        self.assertEqual(account_id1, account_id2)
        # TODO: Ensure that the relevant events were reported.
        #self.assertEquals(2, self.report_mock.user_logged_in.call_count)
        #self.assertEquals(1, self.report_mock.user_registered.call_count)

        # Verify with a new account.
        identifier = '+109999999'
        code3 = self.request_code(identifier)
        result, status = self.post('/v30/challenge/respond', identifier=identifier,
                                   secret=code3)
        self.assertValidResult(result, status, 200)
        self.assertIn('account', result)
        account_id3 = result['account']['id']
        self.assertNotEqual(code1, code2)
        self.assertNotEqual(account_id1, account_id3)
        # TODO: Ensure that the relevant events were reported.
        #self.assertEquals(3, self.report_mock.user_logged_in.call_count)
        #self.assertEquals(2, self.report_mock.user_registered.call_count)

    def test_challenge_respond_invalid_email(self):
        # Test the e-mail challenge flow.
        identifier = 'r+11@g.co'
        # Incorrect code.
        result, status = self.post('/v30/challenge/respond', identifier=identifier,
                                   secret='yolo')
        self.assertValidResult(result, status, 400)
        # Empty code.
        result, status = self.post('/v30/challenge/respond', identifier=identifier,
                                   secret='')
        self.assertValidResult(result, status, 400)
        # Missing code.
        result, status = self.post('/v30/challenge/respond', identifier=identifier)
        self.assertValidResult(result, status, 400)

        # TODO: Ensure that nothing was reported.
        #self.assertEquals(0, self.report_mock.user_logged_in.call_count)
        #self.assertEquals(0, self.report_mock.user_registered.call_count)

    def test_challenge_respond_invalid_phone(self):
        # Test the phone challenge flow.
        identifier = '+11235433456'
        # Incorrect code without requesting one first.
        result, status = self.post('/v30/challenge/respond', identifier=identifier,
                                   secret='yolo')
        self.assertValidResult(result, status, 400)
        # Incorrect code after requesting one.
        code = self.request_code(identifier)
        result, status = self.post('/v30/challenge/respond', identifier=identifier,
                                   secret='yolo')
        self.assertValidResult(result, status, 400)
        # Empty code.
        result, status = self.post('/v30/challenge/respond', identifier=identifier,
                                   secret='')
        self.assertValidResult(result, status, 400)
        # Missing code.
        result, status = self.post('/v30/challenge/respond', identifier=identifier)
        self.assertValidResult(result, status, 400)

        # TODO: Ensure that nothing was reported.
        #self.assertEquals(0, self.report_mock.user_logged_in.call_count)
        #self.assertEquals(0, self.report_mock.user_registered.call_count)

    def test_challenge_respond_email(self):
        identifier = 'r@g.co'
        code = self.request_code(identifier)
        result, status = self.post('/v30/challenge/respond', identifier=identifier,
                                   secret=code)
        self.assertValidResult(result, status, 200)
        self.assertIsValidSession(result, expected_status='active')
        account_id = result.get('account').get('id')
        # TODO: Ensure that the relevant events were reported.
        #self.assertEquals(1, self.report_mock.user_logged_in.call_count)
        #self.assertEquals(1, self.report_mock.user_registered.call_count)

    def test_challenge_respond_expired_token(self):
        identifier = '+11235433456'
        code = self.request_code(identifier)
        with mock.patch('roger.models.datetime') as datetime_mock:
            fake_time = datetime.utcnow()
            fake_time += config.CHALLENGE_TTL
            fake_time += timedelta(minutes=1)
            datetime_mock.utcnow.return_value = fake_time
            result, status = self.post('/v30/challenge/respond', identifier=identifier,
                                       secret=code)
        self.assertValidResult(result, status, 400)
        # TODO: Ensure that nothing was reported.
        #self.assertEquals(0, self.report_mock.user_logged_in.call_count)
        #self.assertEquals(0, self.report_mock.user_registered.call_count)

    def test_challenge_respond_missing_details(self):
        result, status = self.post('/v30/challenge/respond')
        self.assertValidResult(result, status, 400)
        result, status = self.post('/v30/challenge/respond', identifier='+11235433456')
        self.assertValidResult(result, status, 400)
        result, status = self.post('/v30/challenge/respond', secret='12345')
        self.assertValidResult(result, status, 400)
        # TODO: Ensure that nothing was reported.
        #self.assertEquals(0, self.report_mock.user_logged_in.call_count)

    def test_challenge_respond_phone(self):
        identifier = '+11235433456'
        code = self.request_code(identifier)
        result, status = self.post('/v30/challenge/respond', identifier=identifier,
                                   secret=code)
        self.assertValidResult(result, status, 200)
        self.assertIsValidSession(result, expected_status='active')
        account_id = result.get('account').get('id')
        # TODO: Ensure that the relevant events were reported.
        #self.assertEquals(1, self.report_mock.challenge_request.call_count)
        #self.assertEquals(1, self.report_mock.user_logged_in.call_count)
        #self.assertEquals(1, self.report_mock.user_registered.call_count)

    def test_challenge_respond_phone_twice(self):
        identifier = '+11235433456'
        code1 = self.request_code(identifier)
        code2 = self.request_code(identifier)
        self.assertEquals(code1, code2)
        result, status = self.post('/v30/challenge/respond', identifier=identifier,
                                   secret=code2)
        self.assertValidResult(result, status, 200)
        self.assertIsValidSession(result, expected_status='active')
        account_id = result.get('account').get('id')

    def test_challenge_respond_username(self):
        # Ensure that response is 400 if username provided.
        identifier = 'username'
        result, status = self.post('/v30/challenge/respond', identifier=identifier,
                                   secret='yolo')
        self.assertValidResult(result, status, 400)
        # TODO: Ensure that nothing was reported.
        #self.assertEquals(0, self.report_mock.user_logged_in.call_count)
        #self.assertEquals(0, self.report_mock.user_registered.call_count)

    def test_challenge_username(self):
        # Ensure that response is invalid if username identifier provided.
        result, status = self.post('/v30/challenge', identifier='yoooo')
        self.assertValidResult(result, status, 400)
        self.assertFalse(self.send_sms_mock.called)
        self.assertFalse(self.send_email_mock.called)
        # TODO: Ensure that nothing was reported.
        #self.assertEquals(0, self.report_mock.challenge_request.call_count)


class Batch(BaseTestCase):
    @unittest.skip('Batch has been disabled.')
    def test_creating_new_stream_onboarding(self):
        result, status = self.post('/v30/batch?show_in_recents=true',
                                   access_token=self.anna.create_access_token(),
                                   participant=['bob', 'cecilia'])
        self.assertValidResult(result, status, 200)
        self.assertIn('data', result)
        self.assertEquals(len(result['data']), 2)
        # verify bob's stream
        self.assertEquals(len(result['data'][1]['others']), 1)
        self.assertEquals(result['data'][1]['others'][0], to_dict(self.bob.account, version=30, view_account=self.anna))
        # verify cecilia's stream
        self.assertEquals(len(result['data'][0]['others']), 1)
        self.assertEquals(result['data'][0]['others'][0], to_dict(self.cecilia.account, version=30, view_account=self.anna))
        # verify that the streams are on the creator's recents
        result, status = self.get('/v30/streams',
                                  access_token=self.anna.create_access_token())
        self.assertIn('data', result)
        self.assertEquals(len(result['data']), 2)
        # verify bob's stream
        self.assertEquals(len(result['data'][1]['others']), 1)
        self.assertEquals(result['data'][1]['others'][0], to_dict(self.bob.account, version=30, view_account=self.anna))
        # verify cecilia's stream
        self.assertEquals(len(result['data'][0]['others']), 1)
        self.assertEquals(result['data'][0]['others'][0], to_dict(self.cecilia.account, version=30, view_account=self.anna))
        # verify that the stream is in other participants recents
        result, status = self.get('/v30/streams',
                                  access_token=self.bob.create_access_token())
        self.assertIn('data', result)
        self.assertEquals(len(result['data']), 1)
        self.assertEquals(len(result['data'][0]['others']), 1)
        self.assertEquals(result['data'][0]['others'][0], to_dict(self.anna.account, version=30, view_account=self.bob))
        result, status = self.get('/v30/streams',
                                  access_token=self.cecilia.create_access_token())
        self.assertIn('data', result)
        self.assertEquals(len(result['data']), 1)
        self.assertEquals(len(result['data'][0]['others']), 1)
        self.assertEquals(result['data'][0]['others'][0], to_dict(self.anna.account, version=30, view_account=self.cecilia))

    @unittest.skip('Batch has been disabled.')
    def test_batch_skip_self(self):
        result, status = self.post('/v30/batch?show_in_recents=true',
                                   access_token=self.anna.create_access_token(),
                                   participant=['anna', 'bob'])
        self.assertValidResult(result, status, 200)
        self.assertIn('data', result)
        self.assertEquals(len(result['data']), 1)


class Contacts(BaseTestCase):
    def test_only_returns_active(self):
        contacts = [
            ('+12345678',       'active'),
            ('bobby',           'active'),
            ('bob@example.com', 'active'),
            ('+23456789',       'active'),
            ('+87654321',       'inactive'),
            ('+76543218',       'requested'),
            ('+65432187',       'temporary'),
        ]
        provided = [(i, accounts.create(i, status=s)) for i, s in contacts]
        response = self.client.open(
            '/v30/contacts', method='POST',
            headers={'Authorization': 'Bearer %s' % (self.anna.create_access_token(),)},
            data='\n'.join(i for i, s in contacts))
        result, status = json.loads(response.data), response.status_code
        self.assertValidResult(result, status, 200)
        self.assertIn('map', result)
        # Only expect there to be phone numbers / e-mails in the map.
        expected_map = {identifiers.clean(i): {'id': a.account_id, 'active': a.is_active}
                        for i, a in provided
                        if i != 'bobby'}
        self.assertEqual(result['map'], expected_map)


class Content(BaseTestCase):
    def test_create(self):
        result, status = self.post('/v42/content',
                                   access_token=self.anna.create_access_token(),
                                   duration='12345',
                                   tags='recording',
                                   url='https://storage.googleapis.com/rcam/F3CBDwQ4gzX2UQlG4t57x')
        self.assertValidResult(result, status, 200)

    def test_create_original(self):
        result, status = self.put('/v51/original',
                                  title=u'Drake – God’s Plan (Official Music Video) - YouTube',
                                  url='https://www.youtube.com/embed/XUqRem0W8L8?autoplay=1')
        self.assertValidResult(result, status, 200)
        self.assertEqual(result['content']['title'], u'Drake – God’s Plan (Official Music Video)')
        self.assertEqual(result['content']['original_url'], 'https://www.youtube.com/watch?v=XUqRem0W8L8')
        content_id = result['content']['id']
        # Make sure we can find the content by its original URL.
        result, status = self.get('/v51/content', url='https://www.youtube.com/embed/XUqRem0W8L8?autoplay=1')
        self.assertValidResult(result, status, 200)
        self.assertEqual(result['content']['id'], content_id)

    @unittest.skip('Content lists are disabled.')
    def test_delete(self):
        result, status = self.post('/v42/content',
                                   access_token=self.anna.create_access_token(),
                                   duration='12345',
                                   tags='reaction',
                                   url='https://storage.googleapis.com/rcam/F3CBDwQ4gzX2UQlG4t57x')
        self.assertValidResult(result, status, 200)
        content_id = result['content']['id']
        # Confirm that content is in own profile content list.
        result, status = self.get('/v42/profile/me/content/reaction/',
                                  access_token=self.anna.create_access_token())
        self.assertValidResult(result, status, 200)
        for item in result['data']:
            if item['content']['id'] == content_id:
                break
        else:
            self.fail('content not found')
        # Delete it.
        result, status = self.post('/v42/content/%d' % (content_id,),
                                   tags='deleted',
                                   access_token=self.anna.create_access_token())
        self.assertValidResult(result, status, 200)
        # Confirm that it's gone from own profile content list.
        result, status = self.get('/v42/profile/me/content/reaction/',
                                  access_token=self.anna.create_access_token())
        self.assertValidResult(result, status, 200)
        for item in result['data']:
            if item['content']['id'] == content_id:
                self.fail('content was visible')

    def test_get_public_content(self):
        result, status = self.post('/v42/content',
                                   access_token=self.anna.create_access_token(),
                                   duration='12345',
                                   tags='reaction',
                                   url='https://storage.googleapis.com/rcam/F3CBDwQ4gzX2UQlG4t57x')
        self.assertValidResult(result, status, 200)
        content_id = result['content']['id']
        result, status = self.get('/v42/content/reaction/')
        self.assertValidResult(result, status, 200)
        for item in result['data']:
            if item['content']['id'] == content_id:
                break
        else:
            self.fail('content not found')

    def test_get_subs(self):
        # Confirm that feed is empty.
        result, status = self.get('/v42/profile/me/following/content/reaction/',
                                  access_token=self.bob.create_access_token())
        self.assertValidResult(result, status, 200)
        self.assertItemsEqual(result['data'], [])
        # Create one content by another user.
        result, status = self.post('/v42/content',
                                   access_token=self.dennis.create_access_token(),
                                   duration='12345',
                                   tags='reaction',
                                   url='https://storage.googleapis.com/rcam/F3CBDwQ4gzX2UQlG4t57x')
        self.assertValidResult(result, status, 200)
        content_id = result['content']['id']
        # Follow that user.
        result, status = self.put('/v42/profile/me/following/dennis',
                                  access_token=self.bob.create_access_token())
        self.assertValidResult(result, status, 200)
        # Check first item in feed.
        result, status = self.get('/v42/profile/me/following/content/reaction/',
                                  access_token=self.bob.create_access_token())
        self.assertValidResult(result, status, 200)
        self.assertEqual(len(result['data']), 1)
        self.assertEqual(result['data'][0]['content']['id'], content_id)
        # Create content from own account.
        result, status = self.post('/v42/content',
                                   access_token=self.bob.create_access_token(),
                                   duration='12345',
                                   tags='reaction',
                                   url='https://storage.googleapis.com/rcam/F3CBDwQ4gzX2UQlG4t57x')
        self.assertValidResult(result, status, 200)
        content_id = result['content']['id']
        # Check first item in feed.
        result, status = self.get('/v42/profile/me/following/content/reaction/',
                                  access_token=self.bob.create_access_token())
        self.assertValidResult(result, status, 200)
        self.assertEqual(len(result['data']), 2)
        self.assertEqual(result['data'][0]['content']['id'], content_id)

    def test_tag_internal(self):
        # Try an internal tag.
        result, status = self.post('/v42/content',
                                   access_token=self.bob.create_access_token(),
                                   duration='12345',
                                   tags='flagged',
                                   url='https://storage.googleapis.com/rcam/F3CBDwQ4gzX2UQlG4t57x')
        self.assertValidResult(result, status, 400)

    def test_tag_restricted(self):
        # Try a restricted tag.
        result, status = self.post('/v42/content',
                                   access_token=self.bob.create_access_token(),
                                   duration='12345',
                                   tags='featured',
                                   url='https://storage.googleapis.com/rcam/F3CBDwQ4gzX2UQlG4t57x')
        self.assertValidResult(result, status, 400)

    def test_update_tags(self):
        result, status = self.post('/v42/content',
                                   access_token=self.anna.create_access_token(),
                                   duration='12345',
                                   tags='recording',
                                   url='https://storage.googleapis.com/rcam/F3CBDwQ4gzX2UQlG4t57x')
        self.assertValidResult(result, status, 200)
        self.assertEqual(result['content']['tags'], ['recording'])
        content_id = result['content']['id']
        result, status = self.post('/v42/content/%d' % (content_id,),
                                   access_token=self.anna.create_access_token(),
                                   tags='reaction')
        self.assertValidResult(result, status, 200)
        self.assertEqual(result['content']['tags'], ['reaction'])

    def test_view(self):
        result, status = self.post('/v42/content',
                                   access_token=self.anna.create_access_token(),
                                   duration='12345',
                                   tags='recording',
                                   url='https://storage.googleapis.com/rcam/F3CBDwQ4gzX2UQlG4t57x')
        self.assertValidResult(result, status, 200)
        self.assertEqual(result['content']['views'], 0)
        content_id = result['content']['id']
        # First we should get a 404 because the content is not yet visible.
        result, status = self.put('/v42/content/%d/views' % (content_id,))
        self.assertValidResult(result, status, 404)
        # Make the content visible.
        result, status = self.post('/v42/content/%d' % (content_id,),
                                   tags='reaction',
                                   access_token=self.anna.create_access_token())
        self.assertEqual(result['content']['votes'], 0)
        # Mark content viewed.
        result, status = self.put('/v42/content/%d/views' % (content_id,))
        self.assertValidResult(result, status, 200)
        # Check view count.
        result, status = self.get('/v42/content/%d' % (content_id,))
        self.assertValidResult(result, status, 200)
        self.assertEqual(result['content']['views'], 1)

    def test_vote(self):
        result, status = self.post('/v42/content',
                                   access_token=self.anna.create_access_token(),
                                   duration='12345',
                                   tags='recording',
                                   url='https://storage.googleapis.com/rcam/F3CBDwQ4gzX2UQlG4t57x')
        self.assertValidResult(result, status, 200)
        self.assertEqual(result['content']['votes'], 0)
        content_id = result['content']['id']
        # First we should get a 404 because the content is not yet visible.
        result, status = self.put('/v42/content/%d/votes' % (content_id,),
                                  access_token=self.bob.create_access_token())
        self.assertValidResult(result, status, 404)
        # Make the content visible.
        result, status = self.post('/v42/content/%d' % (content_id,),
                                   tags='reaction',
                                   access_token=self.anna.create_access_token())
        self.assertEqual(result['content']['votes'], 0)
        # Ensure that we can vote on it.
        result, status = self.put('/v42/content/%d/votes' % (content_id,),
                                  access_token=self.bob.create_access_token())
        self.assertValidResult(result, status, 200)
        self.assertEqual(result['content']['votes'], 1)
        # Don't count double voting.
        result, status = self.put('/v42/content/%d/votes' % (content_id,),
                                  access_token=self.bob.create_access_token())
        self.assertValidResult(result, status, 200)
        self.assertEqual(result['content']['votes'], 1)


class DeviceToken(BaseTestCase):
    def test_invalid_platform(self):
        # Ensure that an invalid platform gives "Bad Request".
        token = self.anna.create_access_token()
        result, status = self.post('/v30/device', device_token='abcdef', platform='ikea',
                                   access_token=token)
        self.assertValidResult(result, status, 400)

    def test_tokens_replaced(self):
        anna = self.anna.create_access_token()
        bob = self.bob.create_access_token()
        # Create a device token for Anna that we'll check later.
        self.post('/v30/device', client_id='fika', device_id='phone-a1',
                  device_token='abcde', platform='gcm', access_token=anna)
        # Store two tokens for Bob.
        self.post('/v30/device', client_id='fika', device_id='phone-b2',
                  device_token='bcdef', platform='gcm', access_token=bob)
        self.post('/v30/device', client_id='fika', device_id='phone-b2',
                  device_token='cdefg', platform='gcm', access_token=bob)
        # Ensure that only the latest device token is returned for Bob.
        result, status = self.get('/v30/device', access_token=bob)
        self.assertValidResult(result, status, 200)
        self.assertEqual(len(result['data']), 1)
        self.assertEqual(result['data'][0]['token'], 'cdefg')
        # Make sure the token doesn't disappear if it's added a second time.
        self.post('/v30/device', client_id='fika', device_id='phone-b2',
                  device_token='cdefg', platform='gcm', access_token=bob)
        result, status = self.get('/v30/device', access_token=bob)
        self.assertValidResult(result, status, 200)
        self.assertEqual(len(result['data']), 1)
        self.assertEqual(result['data'][0]['token'], 'cdefg')
        # Verify that the same token gets updated if using a different version.
        self.post('/v31/device', client_id='fika', device_id='phone-b2',
                  device_token='cdefg', platform='gcm', access_token=bob)
        result, status = self.get('/v31/device', access_token=bob)
        self.assertValidResult(result, status, 200)
        self.assertEqual(len(result['data']), 1)
        self.assertEqual(result['data'][0]['token'], 'cdefg')
        self.assertEqual(result['data'][0]['api_version'], 31)
        # Check that the device token was persisted for Anna.
        result, status = self.get('/v30/device', access_token=anna)
        self.assertValidResult(result, status, 200)
        self.assertEqual(len(result['data']), 1)
        self.assertEqual(result['data'][0]['token'], 'abcde')
        # Make sure that Anna's token disappears if Bob registers it.
        self.post('/v30/device', client_id='fika', device_id='phone-b2',
                  device_token='abcde', platform='gcm', access_token=bob)
        result, status = self.get('/v30/device', access_token=anna)
        self.assertValidResult(result, status, 200)
        self.assertEqual(len(result['data']), 0)
        # Make sure that Bob can register a second token for a different device.
        self.post('/v30/device', client_id='fika', device_id='phone-c3',
                  device_token='defgh', platform='gcm', access_token=bob)
        result, status = self.get('/v30/device', access_token=bob)
        self.assertValidResult(result, status, 200)
        tokens = map(lambda d: d['token'], result['data'])
        self.assertItemsEqual(tokens, ['abcde', 'defgh'])

    def test_valid_request(self):
        # Ensure that a valid platform and token gives "OK".
        token = self.anna.create_access_token()
        result, status = self.post('/v30/device', client_id='fika', device_token='abcdef',
                                   platform='gcm', access_token=token)
        self.assertValidResult(result, status, 200)
        self.assertTrue(result.get('success'))
        device = models.Device.query(ancestor=self.anna.account.key).get()
        self.assertEquals(device.token, 'abcdef')
        self.assertEquals(device.platform, 'gcm')
        self.assertEquals(device.api_version, 30)


class Invite(BaseTestCase):
    @mock.patch('roger.report.events')
    def test_invite_reported(self, events_mock):
        token = self.anna.create_access_token()
        result, status = self.post('/v30/invite', identifier='+3475550105',
                                   access_token=token)
        self.assertValidResult(result, status, 200)
        self.assertEquals(1, events_mock.InviteV1.call_count)


class Location(BaseTestCase):
    def test_get_self_location(self):
        self.anna.account.set_location(ndb.GeoPt(40.722273, -73.994205), defer=False)
        result, status = self.get('/v30/profile/me',
                                  access_token=self.anna.create_access_token())
        self.assertValidResult(result, status, 200)
        self.assertIsNotNone(result['location'])
        self.assertIsNotNone(result['timezone'])

    def test_get_stream_participant_location(self):
        self.bob.account.set_location(ndb.GeoPt(40.722273, -73.994205), defer=False)
        # Create a stream with bob.
        result, status = self.send_chunk(self.anna, ['bob'])
        self.assertValidResult(result, status, 200)
        self.assertEquals(result.get('others')[0].get('username'), 'bob')
        self.assertIsNotNone(result.get('others')[0].get('location'))
        self.assertIsNotNone(result.get('others')[0].get('timezone'))

    def test_enable_glimpses(self):
        # Verify the recents list.
        result, status = self.get('/v30/streams',
                                  access_token=self.anna.create_access_token())
        self.assertValidResult(result, status, 200)
        found = False
        for stream in result['data']:
            for participant in stream['others']:
                if 'roger' in participant['identifiers']:
                    found = True
            if found:
                self.assertEquals(1, len(stream['chunks']))
        # Update Anna's settings.
        result, status = self.post('/v30/profile/me', share_location='true',
                                   access_token=self.anna.create_access_token())
        self.assertValidResult(result, status, 200)
        # Verify the recents list.
        result, status = self.get('/v30/streams',
                                  access_token=self.anna.create_access_token())
        self.assertValidResult(result, status, 200)

        found = False
        for stream in result['data']:
            for participant in stream['others']:
                if 'roger' in participant['identifiers']:
                    found = True
            if found:
                self.assertEquals(2, len(stream['chunks']))

    def test_update_demographics(self):
        anna_token = self.anna.create_access_token()
        result, status = self.get('/v52/profile/me', access_token=anna_token)
        self.assertValidResult(result, status, 200)
        self.assertIsNone(result['birthday'])
        self.assertIsNone(result['gender'])
        result, status = self.post('/v52/profile/me', gender='female', access_token=anna_token)
        self.assertValidResult(result, status, 200)
        self.assertEqual(result['gender'], 'female')
        result, status = self.get('/v52/profile/me', access_token=anna_token)
        self.assertValidResult(result, status, 200)
        self.assertEqual(result['gender'], 'female')
        result, status = self.post('/v52/profile/me', birthday='1990-07-31', gender='other',
                                   access_token=anna_token)
        self.assertValidResult(result, status, 200)
        self.assertEqual(result['birthday'], '1990-07-31')
        self.assertEqual(result['gender'], 'other')
        # Duplicate request should be a no-op.
        result, status = self.post('/v52/profile/me', birthday='1990-07-31', gender='other',
                                   access_token=anna_token)
        self.assertValidResult(result, status, 200)
        self.assertEqual(result['birthday'], '1990-07-31')
        self.assertEqual(result['gender'], 'other')
        # Shouldn't be able to see anyone else's demographics.
        result, status = self.get('/v52/profile/anna', access_token=self.bob.create_access_token())
        self.assertValidResult(result, status, 200)
        self.assertNotIn('birthday', result)
        self.assertNotIn('gender', result)

    def test_update_share_location(self):
        # Update Anna's settings.
        result, status = self.post('/v30/profile/me', share_location='false',
                                   access_token=self.anna.create_access_token())
        self.assertValidResult(result, status, 200)
        # Shouldn't be able to see own location.
        result, status = self.get('/v30/profile/me',
                                  access_token=self.anna.create_access_token())
        self.assertValidResult(result, status, 200)
        self.assertIsNone(result['location'])
        self.assertIsNone(result['timezone'])
        # Shouldn't be able to see anyone else's location.
        result, status = self.send_chunk(self.anna, ['bob'])
        self.assertValidResult(result, status, 200)
        self.assertEquals(result.get('others')[0].get('username'), 'bob')
        self.assertIsNone(result.get('others')[0].get('location'))
        self.assertIsNone(result.get('others')[0].get('timezone'))

    def test_public_profile_location_hidden(self):
        # Shouldn't be able to get a public profile's location.
        self.anna.account.set_location(ndb.GeoPt(40.722273, -73.994205), defer=False)
        result, status = self.get('/v30/profile/anna')
        self.assertValidResult(result, status, 200)
        self.assertNotIn('location', result)
        self.assertNotIn('timezone', result)


class Onboarding(BaseTestCase):
    def test_sign_up_with_initial_participant(self):
        identifier = '+16461230159'
        code = self.request_code(identifier)
        result, status = self.post('/v30/challenge/respond',
                                   identifier=identifier, secret=code,
                                   stream_participant='anna')
        self.assertValidResult(result, status, 200)
        handler = accounts.get_handler(identifier)
        # Ensure that there is a stream already.
        self.assertEqual(len(result['streams']), 1)
        # Ensure that the stream has the right participants.
        self.assertIn('others', result['streams'][0])
        self.assertItemsEqual(
            [o['id'] for o in result['streams'][0]['others']],
            [self.anna.account_id])
        # The public profile user should have some idea about this.
        recents, _ = self.anna.streams.get_recent()
        found = False
        for stream in recents:
            if stream.has_participant(result['account']['id']):
                found = True
        self.assertTrue(found)

    def test_sign_up_without_initial_participant(self):
        identifier = '+16461230160'
        code = self.request_code(identifier)
        result, status = self.post('/v30/challenge/respond',
                                   identifier=identifier, secret=code)
        self.assertValidResult(result, status, 200)
        handler = accounts.get_handler(identifier)
        # Ensure that there is a stream already.
        self.assertEqual(len(result['streams']), 0)


class PublicRequests(BaseTestCase):
    ORIGINAL_TITLE = 'Rick Astley - Never Gonna Give You Up [HQ]'
    ORIGINAL_URL = 'https://www.youtube.com/watch?v=DLzxrzFCyOs'

    def create_public_request(self, add_reward=None, approve=False):
        result, status = self.post('/v54/requests/public/',
                                   access_token=self.anna.create_access_token(),
                                   content_title=self.ORIGINAL_TITLE,
                                   content_url=self.ORIGINAL_URL,
                                   tags='default')
        self.assertValidResult(result, status, 200)
        r = models.ContentRequestPublic.get_by_id(result['id'])
        needs_put = False
        if add_reward is not None:
            wallet = models.Wallet.create_internal(r.requested_by,
                                                   'request_%d_reward' % (r.key.id(),),
                                                   add_reward,
                                                   'Request reward pool')
            r.wallet = wallet.key
            needs_put = True
        if approve:
            r.tags = ['approved', 'default']
            needs_put = True
        if needs_put:
            r.put()
        return r

    def test_create_entry(self):
        # Create approved request.
        r = self.create_public_request(approve=True)
        # Check request details.
        result, status = self.get('/v54/requests/public/%d' % (r.key.id(),),
                                  access_token=self.bob.create_access_token())
        self.assertValidResult(result, status, 200)
        self.assertEqual(result['status'], 'open')
        # Create an entry for the request.
        result, status = self.post('/v54/requests/public/%d/entry' % (r.key.id(),),
                                   access_token=self.bob.create_access_token())
        self.assertValidResult(result, status, 200)
        # Ensure that creating the entry updated the status.
        result, status = self.get('/v54/requests/public/%d' % (r.key.id(),),
                                  access_token=self.bob.create_access_token())
        self.assertValidResult(result, status, 200)
        self.assertEqual(result['status'], 'pending-upload')

    def test_create_request(self):
        r = self.create_public_request()
        # Make sure list is empty.
        result, status = self.get('/v54/requests/public/default/',
                                  access_token=self.bob.create_access_token())
        self.assertValidResult(result, status, 200)
        self.assertEqual(result['data'], [])
        # Approve the request.
        r.tags = ['approved', 'default']
        r.put()
        # Clear cache to get fresh results.
        self.clear_memcache()
        # Make sure list has one item.
        result, status = self.get('/v54/requests/public/default/',
                                  access_token=self.bob.create_access_token())
        self.assertValidResult(result, status, 200)
        self.assertEqual(len(result['data']), 1)
        self.assertEqual(result['data'][0]['content']['title'], self.ORIGINAL_TITLE)
        self.assertEqual(result['data'][0]['request']['id'], r.key.id())

    def test_invalid_request(self):
        result, status = self.post('/v54/requests/public/',
                                   access_token=self.anna.create_access_token(),
                                   content_title='Rick Astley - Never Gonna Give You Up [HQ]',
                                   content_url='https://www.youtube.com/watch?v=DLzxrzFCyOs',
                                   tags='approved,default')
        self.assertValidResult(result, status, 501)

    def test_review_entry(self):
        # Create approved request with a reward pool.
        r = self.create_public_request(add_reward=100, approve=True)
        # Create an entry that's pending upload.
        # TODO: Ideally this should be created via the POST /vxx/requests/public/.../entry endpoint.
        content = models.Content.new(allow_restricted_tags=True,
            creator=self.bob.key,
            related_to=r.content,
            request=r.key,
            tags=['is hidden'],
            thumb_url='https://www.example.com/image.jpg',
            title='My reaction to the best song ever',
            youtube_id_history=['aBcDeF87'],
            youtube_views=5,
            youtube_views_updated=datetime.utcnow())
        content.put()
        e, _ = models.ContentRequestPublicEntry.update((r.key, self.bob.key), content)
        # Ensure that the entry exists and has the correct status.
        result, status = self.get('/v54/requests/public/%d' % (r.key.id(),),
                                  access_token=self.bob.create_access_token())
        self.assertValidResult(result, status, 200)
        self.assertEqual(result['status'], 'pending-review')
        # Attempt to reward a non-active entry.
        content.youtube_views = 10
        content.put()
        amount = models.ContentRequestPublicEntry.reward(e.key, content, r.wallet_owner, r.wallet)
        self.assertEqual(amount, 0)
        # Approve the entry.
        e = models.ContentRequestPublicEntry.review(e.key, True)
        self.assertEqual(e.status, 'active')
        # Attempt reward again.
        amount = models.ContentRequestPublicEntry.reward(e.key, content, r.wallet_owner, r.wallet)
        self.assertEqual(amount, 5)
        # Finally, attempt to reward more than is available.
        content.youtube_views = 1000
        content.put()
        amount = models.ContentRequestPublicEntry.reward(e.key, content, r.wallet_owner, r.wallet)
        self.assertEqual(amount, 95)


class Services(BaseTestCase):
    def setUp(self):
        super(Services, self).setUp()

        self.service_1 = models.Service(id='svc1', connect_url='https://example.com',
                                        featured=10, title='Service #1',
                                        categories=['bot'])
        self.service_1.put()
        self.service_2 = models.Service(id='svc2', connect_url='https://example.com',
                                        featured=20, title='Service #2',
                                        categories=['service'])
        self.service_2.put()
        self.service_3 = models.Service(id='svc3', connect_url='https://example.com',
                                        title='Service #3',
                                        categories=['service'])
        self.service_3.put()
        self.service_4 = models.Service(id='svc4', connect_url='https://example.com',
                                        featured=30, title='Service #4',
                                        categories=['service', 'service_employee'])
        self.service_4.put()
        self.service_5 = models.Service(id='svc5', connect_url='https://example.com',
                                        featured=40, title='Service #5',
                                        categories=['service_employee'])
        self.service_5.put()

    def test_auth_service(self):
        # Verify that Service #3 is not in the services list.
        result, status = self.get('/v30/services',
                                  access_token=self.anna.create_access_token())
        self.assertValidResult(result, status, 200)
        for service in result['data']:
            # Nothing is connected yet.
            self.assertFalse(service['connected'], False)
            # Non-featured service should not be there.
            self.assertNotEqual(service['id'], self.service_3.key.id())
        # This would happen externally...
        self.anna.connect_service(self.service_3, None, 'anna', access_token='xyz',
                                  client='test', expires_in=3600)
        self.anna.connect_service(self.service_4, None, 'anna', access_token='xyz',
                                  client='test', expires_in=3600)
        # Verify that we now also get the previously hidden connected service.
        result, status = self.get('/v30/services',
                                  access_token=self.anna.create_access_token())
        self.assertValidResult(result, status, 200)
        self.assertEqual(len(result['data']), 3)
        # Featured, connected services should be first.
        self.assertEqual(result['data'][0]['id'], self.service_4.key.id())
        self.assertTrue(result['data'][0]['connected'])
        # Connected services should come before non-connected, featured ones.
        self.assertEqual(result['data'][1]['id'], self.service_3.key.id())
        self.assertTrue(result['data'][1]['connected'])

    @mock.patch('roger.youtube._fetch_async')
    def test_auth_youtube(self, fetch_mock):
        def fetch(url, *args, **kwargs):
            m = mock.Mock()
            m.status_code = 200
            if url.startswith('https://upload.reaction.cam/v2/channel'):
                m.content = (
                    '{"channels":[{'
                    '"comments":402'
                    ','
                    '"id":"aChan123"'
                    ','
                    '"subs":100'
                    ','
                    '"thumb_url":"http://x.com/i.jpg"'
                    ','
                    '"title":"Hello World"'
                    ','
                    '"videos":5'
                    ','
                    '"views":12531'
                    '}]}')
            elif url.startswith('https://www.googleapis.com/oauth2/v4/token'):
                profile = {'sub': 'abc123', 'email': 'anna@example.com', 'email_verified': True}
                values = ['header', json.dumps(profile), 'sig']
                id_token = '.'.join(base64.b64encode(v).rstrip('=') for v in values)
                m.content = '{"id_token":"%s"}' % (id_token,)
            f = ndb.Future()
            f.set_result(m)
            return f
        fetch_mock.side_effect = fetch
        result, status = self.post('/v53/youtube/auth',
                                   access_token=self.anna.create_access_token(),
                                   code='HelloWorld')
        self.assertValidResult(result, status, 200)

    def test_get_bots(self):
        # This should return the featured bots only.
        result, status = self.get('/v30/bots',
                                  access_token=self.anna.create_access_token())
        self.assertValidResult(result, status, 200)
        self.assertEqual(len(result['data']), 1)
        self.assertEqual(result['data'][0]['title'], self.service_1.title)

    def test_get_services(self):
        # This should return the featured services only.
        result, status = self.get('/v30/services',
                                  access_token=self.anna.create_access_token())
        self.assertValidResult(result, status, 200)
        self.assertEqual(len(result['data']), 2)
        self.assertEqual(result['data'][0]['title'], self.service_2.title)

    def test_invite(self):
        self.anna.add_identifier('anna@example.com')
        result, status = self.post('/v30/services/email/invite?team_id=example.com',
                                   identifier='fredrik',
                                   access_token=self.anna.create_access_token())
        self.assertValidResult(result, status, 200)

    def test_service_identifiers(self):
        # Without a team.
        service, team, resource = models.Service.parse_identifier('SPOTIFY:bob')
        self.assertEqual((service.id(), team, resource), ('spotify', None, 'bob'))
        # With a team.
        service, team, resource = models.Service.parse_identifier(' Ikea:%22Stuff%22/Hello%20There ')
        self.assertEqual((service.id(), team.id(), resource), ('ikea', '"Stuff"', 'Hello There'))
        service, team, resource = models.Service.parse_identifier('ikea:"Stuff"/Hello There')
        self.assertEqual((service.id(), team.id(), resource), ('ikea', '"Stuff"', 'Hello There'))


class Streams(BaseTestCase):
    def test_attachments(self):
        # Create a stream.
        result, status = self.send_chunk(self.anna, 'bob', title='HiThere', version=30)
        self.assertValidResult(result, status, 200)
        stream_id = result['id']
        # Attempt to create a new attachment with no type specified.
        result, status = self.post('/v30/streams/%d/attachments/testing' % (stream_id,),
                                   access_token=self.anna.create_access_token(),
                                   data=json.dumps({'value': 'Test41'}))
        self.assertValidResult(result, status, 400)
        # Set an attachment on the stream.
        test_data = {'type': 'test', 'value': 'Test42'}
        result, status = self.post('/v30/streams/%d/attachments/testing' % (stream_id,),
                                   access_token=self.anna.create_access_token(),
                                   data=json.dumps(test_data))
        self.assertValidResult(result, status, 200)
        self.assertIn('testing', result['attachments'])
        attachment = result['attachments']['testing']
        self.assertIn('timestamp', attachment)
        self.assertEqual(attachment['account_id'], self.anna.account_id)
        self.assertEqual(attachment['type'], test_data['type'])
        self.assertEqual(attachment['value'], test_data['value'])
        # Attempt to update the same attachment (no type specified).
        new_test_data = {'value': 'Test43', 'extra': 'hello'}
        result, status = self.post('/v30/streams/%d/attachments/testing' % (stream_id,),
                                   access_token=self.anna.create_access_token(),
                                   data=json.dumps(new_test_data))
        self.assertValidResult(result, status, 200)
        test_data.update(new_test_data)
        attachment = result['attachments']['testing']
        self.assertIn('timestamp', attachment)
        self.assertEqual(attachment['account_id'], self.anna.account_id)
        self.assertEqual(attachment['type'], test_data['type'])
        self.assertEqual(attachment['value'], test_data['value'])
        self.assertEqual(attachment['extra'], test_data['extra'])
        # Remove the attachment.
        result, status = self.delete('/v30/streams/%d/attachments/testing' % (stream_id,),
                                     access_token=self.anna.create_access_token())
        self.assertValidResult(result, status, 200)
        self.assertNotIn('testing', result['attachments'])

    def test_autojoin_service_stream(self):
        stream = self.anna.streams.get_or_create([], title='#test',
                                                 service_content_id='ikea:team1/channel1',
                                                 service_members=['user1', 'user2'])
        # Ensure that Bob can't access the stream.
        result, status = self.get('/v30/streams/%d' % (stream.key.id(),),
                                  access_token=self.bob.create_access_token())
        self.assertValidResult(result, status, 403)
        # Create an account that is one of the members.
        johnny = accounts.get_or_create('ikea:team1/user1', display_name='Johnny')
        # Confirm that Johnny can access the stream.
        result, status = self.get('/v30/streams/%d' % (stream.key.id(),),
                                  access_token=johnny.create_access_token())
        self.assertValidResult(result, status, 200)
        # Create an account that is part of the team, but not a content member.
        kelvin = accounts.get_or_create('ikea:team1/user3', display_name='Kelvin')
        # Confirm that Kelvin can't access the stream.
        result, status = self.get('/v30/streams/%d' % (stream.key.id(),),
                                  access_token=kelvin.create_access_token())
        self.assertValidResult(result, status, 403)

    def test_chunk_tokens(self):
        # Try non-existent chunk token.
        result, status = self.get('/v30/profile/anna/chunks/abcde')
        self.assertValidResult(result, status, 404)
        # Create a stream with chunk token.
        result, status = self.send_chunk(self.anna, 'bob', chunk_token='abcde', version=30)
        self.assertValidResult(result, status, 200)
        sid = result['id']
        result, status = self.get('/v30/profile/anna/chunks/abcde')
        self.assertValidResult(result, status, 200)
        self.assertEqual(result['stream_id'], sid)
        # Add another chunk to the stream, including a chunk token.
        result, status = self.send_chunk(self.anna, sid, chunk_token='edcba', version=30)
        result, status = self.get('/v30/profile/anna/chunks/edcba')
        self.assertValidResult(result, status, 200)
        self.assertEqual(result['stream_id'], sid)

    def test_creating_new_stream(self):
        result, status = self.post('/v30/streams',
                                   access_token=self.anna.create_access_token(),
                                   participant=['bob', 'cecilia'], title='ABBA')
        self.assertValidResult(result, status, 200)
        new_stream_id = result['id']
        self.assertIn('others', result)
        self.assertItemsEqual(
            [o['id'] for o in result['others']],
            [self.bob.account_id, self.cecilia.account_id])
        # Verify stream visibility.
        result, status = self.get('/v30/streams',
                                  access_token=self.anna.create_access_token())
        self.assertTrue(any(s['id'] == new_stream_id for s in result['data']))
        result, status = self.get('/v30/streams',
                                  access_token=self.bob.create_access_token())
        self.assertTrue(any(s['id'] == new_stream_id for s in result['data']))
        result, status = self.get('/v30/streams',
                                  access_token=self.cecilia.create_access_token())
        self.assertTrue(any(s['id'] == new_stream_id for s in result['data']))

    @mock.patch('roger.files.gcs')
    def test_creating_new_stream_with_greeting(self, gcs_mock):
        self.bob.set_greeting('testing.mp3', 4000)
        result, status = self.post('/v30/streams',
                                   access_token=self.anna.create_access_token(),
                                   participant=['bob'])
        self.assertValidResult(result, status, 200)
        new_stream_id = result['id']
        self.assertIn('others', result)
        self.assertItemsEqual([o['id'] for o in result['others']], [self.bob.account_id])
        # Verify that the stream is in all participants recents due to greeting.
        result, status = self.get('/v30/streams',
                                  access_token=self.anna.create_access_token())
        for stream in result['data']:
            if new_stream_id == stream['id']:
                break
        else:
            self.fail('new stream not found')
        self.assertTrue(len(stream['chunks']) > 0)
        greeting_chunk = stream['chunks'][0]
        self.assertEqual(greeting_chunk['url'], files.storage_url(self.bob.greeting))
        # The greeting couldn't have started before the stream was created.
        self.assertTrue(greeting_chunk['start'] >= stream['created'])
        # The greeting should be completely unplayed by the creator.
        self.assertTrue(stream['played_until'] < greeting_chunk['start'])
        result, status = self.get('/v30/streams',
                                  access_token=self.bob.create_access_token())
        for stream in result['data']:
            if new_stream_id == stream['id']:
                break
        else:
            self.fail('new stream not found')

    @mock.patch('roger.files.gcs')
    def test_creating_new_stream_with_image(self, gcs_mock):
        result, status = self.post('/v30/streams',
                                   access_token=self.anna.create_access_token(),
                                   image=(StringIO('<image data>'), 'filename.jpg'),
                                   title='Fun Group',
                                   participant=['bob'])
        self.assertValidResult(result, status, 200)
        self.assertTrue(result['image_url'])

    @mock.patch('roger.files.gcs')
    def test_get_or_create_with_existing_stream_and_image(self, gcs_mock):
        # Create a stream without an image.
        result, status = self.post('/v30/streams',
                                   access_token=self.anna.create_access_token(),
                                   participant=['bob'])
        self.assertValidResult(result, status, 200)
        self.assertFalse(result['image_url'])
        stream_id = result['id']
        # Now make the same request with an image.
        result, status = self.post('/v30/streams',
                                   access_token=self.anna.create_access_token(),
                                   image=(StringIO('<image data>'), 'filename.jpg'),
                                   participant=['bob'])
        self.assertValidResult(result, status, 200)
        # The image should now be set.
        self.assertTrue(result['image_url'])
        self.assertEqual(stream_id, result['id'])

    def test_duplicate_content(self):
        result, status = self.send_chunk(self.anna, ['bob'], payload='samesame')
        self.assertValidResult(result, status, 200)
        result, status = self.send_chunk(self.anna, ['bob'], payload='samesame')
        self.assertValidResult(result, status, 403)

    def test_delete_chunk(self):
        # Send a chunk to a stream.
        result, status = self.send_chunk(self.anna, ['bob'], title='Deleting Chunk')
        self.assertValidResult(result, status, 200)
        chunk = result['chunks'][-1]
        # Delete the chunk.
        stream_id, chunk_id = result['id'], chunk['id']
        result, status = self.delete('/v30/streams/%d/chunks/%d' % (stream_id, chunk_id),
                                     access_token=self.anna.create_access_token())
        self.assertValidResult(result, status, 200)
        # Verify that the chunk is not in the stream response.
        result, status = self.get('/v30/streams/%d' % (stream_id,),
                                  access_token=self.bob.create_access_token())
        self.assertValidResult(result, status, 200)
        for chunk in result['chunks']:
            self.assertNotEqual(chunk['id'], chunk_id)
        # Verify that the chunk is not in the chunks list.
        result, status = self.get('/v30/streams/%d/chunks' % (stream_id,),
                                  access_token=self.bob.create_access_token())
        self.assertValidResult(result, status, 200)
        for chunk in result['data']:
            self.assertNotEqual(chunk['id'], chunk_id)

    @mock.patch('roger.files.gcs')
    def test_delete_image(self, gcs_mock):
        # Create a stream.
        result, status = self.post('/v30/streams',
                                   access_token=self.anna.create_access_token(),
                                   image=(StringIO('<image data>'), 'filename.jpg'),
                                   title='Images!',
                                   participant=['bob'])
        self.assertValidResult(result, status, 200)
        self.assertTrue(result['image_url'])
        # Now remove the image.
        result, status = self.delete('/v30/streams/%d/image' % (result['id'],),
                                     access_token=self.anna.create_access_token())
        self.assertValidResult(result, status, 200)
        self.assertIsNone(result['image_url'])

    def test_delete_title(self):
        # Create a stream.
        result, status = self.post('/v30/streams',
                                   access_token=self.anna.create_access_token(),
                                   title='Titles!',
                                   participant=['bob'])
        self.assertValidResult(result, status, 200)
        self.assertTrue(result['title'])
        # Now remove the image.
        result, status = self.delete('/v30/streams/%d/title' % (result['id'],),
                                     access_token=self.anna.create_access_token())
        self.assertValidResult(result, status, 200)
        self.assertIsNone(result['title'])

    def test_get_chunks(self):
        # Send a few chunks to a stream.
        chunks = []
        for i in xrange(10):
            result, status = self.send_chunk(self.anna, ['bob'])
            self.assertValidResult(result, status, 200)
            chunks.append(result['chunks'][-1])
            time.sleep(0.01)
        # Get the chunks endpoint and verify we got all chunks back.
        result, status = self.get('/v30/streams/%s/chunks' % (result.get('id'),),
                                  access_token=self.anna.create_access_token())
        self.assertValidResult(result, status, 200)
        self.assertEqual(result.get('data'), chunks)

    def test_get_recents(self):
        stream_ids = []
        # Create an empty stream.
        result, status = self.post('/v30/streams',
                                   access_token=self.anna.create_access_token(),
                                   participant=['+1-800-555-0123'])
        self.assertValidResult(result, status, 200)
        self.assertFalse(result['chunks'], 'Expected stream to be empty')
        stream_ids.append(result['id'])
        # Create a stream with a payload.
        result, status = self.send_chunk(self.anna, ['bob'])
        self.assertValidResult(result, status, 200)
        self.assertTrue(result['chunks'], 'Expected stream to have chunks')
        self.assertNotIn(result['id'], stream_ids)
        stream_ids.append(result['id'])
        # Verify the recents list.
        result, status = self.get('/v30/streams',
                                  access_token=self.anna.create_access_token())
        self.assertValidResult(result, status, 200)
        # Validate the two stream ids.
        self.assertItemsEqual([s['id'] for s in result['data']], reversed(stream_ids))

    def test_invite_participant(self):
        # Create a named stream.
        result, status = self.send_chunk(self.anna, ['cecilia', 'dennis'],
                                         title='Alpha Flight')
        self.assertValidResult(result, status, 200)
        stream_id = result.get('id')
        self.assertIsNotNone(stream_id)
        # Add a participant.
        result, status = self.post('/v30/streams/%s/participants' % (stream_id,),
                                   access_token=self.anna.create_access_token(),
                                   participant='bob')
        self.assertValidResult(result, status, 200)
        self.assertIn('others', result)
        self.assertItemsEqual(
            [o['id'] for o in result['others']],
            [self.bob.account_id, self.cecilia.account_id, self.dennis.account_id])

    def test_participant_order(self):
        # Create a stream.
        result, status = self.send_chunk(self.anna, ['bob', 'cecilia', 'dennis'],
                                         title='Order v30', version=30)
        self.assertValidResult(result, status, 200)
        self.assertIn('id', result)
        stream_id = result['id']
        self.assertIn('others', result)
        self.assertItemsEqual(
            [o['id'] for o in result['others']],
            [self.bob.account_id, self.cecilia.account_id, self.dennis.account_id])
        # Send a chunk from Dennis, then Bob, then Cecilia to create a specific order.
        self.send_chunk(self.dennis, stream_id, version=30)
        self.send_chunk(self.bob, stream_id, version=30)
        self.send_chunk(self.cecilia, stream_id, version=30)
        # Ensure that Anna sees participants in the order Cecilia, Bob, Dennis.
        result, status = self.get('/v30/streams/%d' % (stream_id,),
                                  access_token=self.anna.create_access_token())
        self.assertValidResult(result, status, 200)
        self.assertIn('others', result)
        self.assertItemsEqual(
            [o['id'] for o in result['others']],
            [self.cecilia.account_id, self.bob.account_id, self.dennis.account_id])
        # Ensure that Bob sees participants in the order Cecilia, Dennis, Anna.
        result, status = self.get('/v30/streams/%d' % (stream_id,),
                                  access_token=self.bob.create_access_token())
        self.assertValidResult(result, status, 200)
        self.assertIn('others', result)
        self.assertItemsEqual(
            [o['id'] for o in result['others']],
            [self.cecilia.account_id, self.dennis.account_id, self.anna.account_id])

    def test_participant_data(self):
        result, status = self.send_chunk(self.anna, ['bob', 'cecilia', 'dennis'],
                                         title='ParticipantData30', version=30)
        self.assertValidResult(result, status, 200)
        self.assertEqual(len(result['others']), 3)

    def test_remove_participant(self):
        # Create a named stream.
        result, status = self.send_chunk(self.anna, ['bob', 'dennis'],
                                         title='Power Rangers')
        self.assertValidResult(result, status, 200)
        self.assertIn('id', result)
        stream_id = result['id']
        # Remove a participant.
        result, status = self.delete('/v30/streams/%s/participants' % (stream_id,),
                                     access_token=self.anna.create_access_token(),
                                     participant='dennis')
        self.assertValidResult(result, status, 200)
        self.assertIn('others', result)
        self.assertItemsEqual(
            [o['id'] for o in result['others']],
            [self.bob.account_id])

    def test_leaving_stream(self):
        # Create a named stream.
        result, status = self.send_chunk(self.bob, ['anna', 'dennis'],
                                         title='Family <3', version=30)
        self.assertValidResult(result, status, 200)
        stream_id = result['id']
        # Leave the stream.
        result, status = self.delete('/v30/streams/%d' % (stream_id,),
                                     access_token=self.bob.create_access_token())
        self.assertValidResult(result, status, 200)
        # Ensure that we can't read the stream.
        result, status = self.get('/v30/streams/%d' % (stream_id,),
                                  access_token=self.bob.create_access_token())
        self.assertValidResult(result, status, 403)
        # Ensure that we can't modify the stream.
        result, status = self.post('/v30/streams/%d' % (stream_id,),
                                   access_token=self.bob.create_access_token(),
                                   title='I HATE YOU')
        self.assertValidResult(result, status, 403)

    def test_reactions(self):
        result, status = self.send_chunk(self.bob, ['anna', 'dennis'], version=36)
        self.assertValidResult(result, status, 200)
        chunk = result['chunks'][-1]
        stream_id, chunk_id = result['id'], chunk['id']
        # Set reaction with v35 and v36 APIs (v35 doesn't allow custom emoji).
        result, status = self.post('/v35/streams/%d/chunks/%d' % (stream_id, chunk_id),
                                   access_token=self.anna.create_access_token(),
                                   reaction='true')
        result, status = self.post('/v36/streams/%d/chunks/%d' % (stream_id, chunk_id),
                                   access_token=self.bob.create_access_token(),
                                   reaction=u'👺')
        self.assertValidResult(result, status, 200)
        result, status = self.get('/v36/streams/%d' % (stream_id,),
                                  access_token=self.bob.create_access_token())
        self.assertValidResult(result, status, 200)
        chunk = result['chunks'][-1]
        self.assertEqual(chunk.get('reactions'), {
            str(self.anna.account_id): u'👍',
            str(self.bob.account_id): u'👺',
        })


    def test_sending_to_phone_contact(self):
        result, status = self.send_chunk(self.bob, 'work:+13475550123,home:+16465550132')
        self.assertValidResult(result, status, 200)

    def test_sending_to_named_group(self):
        # Create an unnamed stream.
        result, status = self.send_chunk(self.anna, ['bob', 'cecilia', 'dennis'])
        self.assertValidResult(result, status, 200)
        self.assertIsNone(result.get('title'))
        unnamed_stream_id = result.get('id')
        # Create a named stream with the same people.
        result, status = self.send_chunk(self.anna, ['bob', 'cecilia', 'dennis'],
                                         title='A-Team')
        self.assertValidResult(result, status, 200)
        self.assertNotEqual(unnamed_stream_id, result.get('id'))
        self.assertEqual(result.get('title'), 'A-Team')
        # Finally try sending to the unnamed one again.
        result, status = self.send_chunk(self.anna, ['bob', 'cecilia', 'dennis'])
        self.assertValidResult(result, status, 200)
        self.assertIsNone(result.get('title'))
        self.assertEqual(result.get('id'), unnamed_stream_id)

    def test_sending_to_new_stream(self):
        result, status = self.send_chunk(self.anna, ['bob', 'cecilia', 'dennis'])
        self.assertValidResult(result, status, 200)
        self.assertIn('others', result)
        self.assertItemsEqual(
            [o['id'] for o in result['others']],
            [self.bob.account_id, self.cecilia.account_id, self.dennis.account_id])

    def test_sending_empty_chunk_should_fail(self):
        result, status = self.send_chunk(self.anna, ['bob'], payload='', version=30)
        self.assertValidResult(result, status, 400)

    def test_sending_to_stream_id_duo(self):
        # Create a stream.
        result, status = self.send_chunk(self.bob, '+13475550191')
        self.assertValidResult(result, status, 200)
        stream_id = result.get('id')
        # Send to the stream id.
        result, status = self.send_chunk(self.bob, stream_id)
        self.assertValidResult(result, status, 200)

    def test_sending_to_stream_id_group(self):
        # Create a stream.
        result, status = self.send_chunk(self.anna, ['bob', 'cecilia', 'dennis'])
        self.assertValidResult(result, status, 200)
        stream_id = result.get('id')
        # Send to the stream id.
        result, status = self.send_chunk(self.dennis, stream_id)
        self.assertValidResult(result, status, 200)

    def test_stream_lookup(self):
        # Create an unnamed stream.
        result, status = self.send_chunk(self.anna, ['bob', 'cecilia', 'dennis'])
        self.assertValidResult(result, status, 200)
        stream_id = result.get('id')
        self.assertIsNotNone(stream_id)
        # Ensure that another participant sending to the group gets the same stream.
        result, status = self.send_chunk(self.bob, ['anna', 'cecilia', 'dennis'])
        self.assertValidResult(result, status, 200)
        self.assertEqual(result['id'], stream_id)


class StreamsInvite(BaseTestCase):
    def test_invite_shareable(self):
        # Create a new stream that is not shareable.
        result, status = self.send_chunk(self.cecilia, [], title='Testing', version=30)
        self.assertValidResult(result, status, 200)
        self.assertFalse(result['invite_token'])
        stream_id = result['id']
        # Make the stream shareable.
        result, status = self.post('/v30/streams/%d' % (stream_id,),
                                   access_token=self.cecilia.create_access_token(),
                                   shareable='true')
        self.assertValidResult(result, status, 200)
        invite_token = result['invite_token']
        self.assertTrue(invite_token)
        # Ensure that the stream can be loaded publicly.
        result, status = self.get('/v30/publicstreams/%s' % (invite_token,))
        self.assertValidResult(result, status, 200)
        # Make the stream non-shareable.
        result, status = self.post('/v30/streams/%d' % (stream_id,),
                                   access_token=self.cecilia.create_access_token(),
                                   shareable='false')
        self.assertFalse(result['invite_token'])
        # Ensure that the stream can NOT be loaded publicly.
        result, status = self.get('/v30/publicstreams/%s' % (invite_token,))
        self.assertValidResult(result, status, 404)

    def test_invite_via_token(self):
        # Create a new stream and make sure it's not shareable.
        result, status = self.send_chunk(self.cecilia, [], shareable='true',
                                         title='HiThere', version=30)
        self.assertValidResult(result, status, 200)
        invite_token = result['invite_token']
        stream_id = result['id']
        # Join the stream as someone else.
        result, status = self.post('/v30/streams',
                                   access_token=self.dennis.create_access_token(),
                                   invite_token=invite_token)
        self.assertValidResult(result, status, 200)
        self.assertEqual(result['id'], stream_id)
        self.assertEqual(len(result['others']), 1)
        self.assertEqual(result['others'][0]['id'], self.cecilia.account.key.id())
        # Ensure attempting to join a second time gives the same result.
        previous_result = result
        result, status = self.post('/v30/streams',
                                   access_token=self.dennis.create_access_token(),
                                   invite_token=invite_token)
        self.assertValidResult(result, status, 200)
        self.assertEqual(result, previous_result)

    def test_public_invite_token(self):
        # Create a new shareable stream.
        result, status = self.send_chunk(self.dennis, [], shareable='true',
                                         title='Bestsies4Ever', version=30)
        self.assertValidResult(result, status, 200)
        title = result['title']
        invite_token = result['invite_token']
        self.assertTrue(invite_token)
        # Get the public representation of the stream.
        result, status = self.get('/v30/publicstreams/%s' % (invite_token,))
        self.assertValidResult(result, status, 200)
        self.assertEqual(result['title'], title)
        # Unlike regular streams, the entire participant list is returned.
        self.assertIn('participants', result)
        self.assertEqual(len(result['participants']), 1)
        self.assertEqual(result['participants'][0]['id'], self.dennis.account_id)


class Profile(BaseTestCase):
    def test_block(self):
        # Send a chunks to a stream.
        result, status = self.send_chunk(self.bob, ['anna'])
        stream_id = result['id']
        # Get the chunks endpoint and verify we got all chunks back.
        result, status = self.get('/v30/streams/%d/chunks' % (stream_id,),
                                  access_token=self.anna.create_access_token())
        self.assertValidResult(result, status, 200)
        self.assertEqual(len(result['data']), 1)
        chunks = result
        # Now block Bob.
        result, status = self.post('/v30/profile/me/blocked',
                                  identifier='bob',
                                  access_token=self.anna.create_access_token())
        self.assertValidResult(result, status, 200)
        self.assertTrue(result['success'])
        # Have Bob try and talk to Anna again.
        result, status = self.send_chunk(self.bob, stream_id)
        # Verify that no additional chunk was sent to Anna.
        result, status = self.get('/v30/streams/%s/chunks' % (result.get('id'),),
                                  access_token=self.anna.create_access_token())
        self.assertValidResult(result, status, 200)
        self.assertEqual(len(result['data']), 1)
        self.assertEqual(result, chunks)
        # Make sure that Bob can't add Anna to a conversation.
        # TODO: This should still work, but the stream shouldn't be visible to Anna.
        stream = self.bob.streams.get_or_create([])
        result, status = self.post('/v30/streams/%d/participants' % (stream.key.id(),),
                                   access_token=self.bob.create_access_token(),
                                   identifier='anna')
        self.assertValidResult(result, status, 200)
        self.assertEqual(len(result['others']), 0)

    def test_block_location(self):
        # set locations
        self.bob.account.set_location(ndb.GeoPt(45.722273, -73.994205), defer=False)
        self.anna.account.set_location(ndb.GeoPt(40.722273, -73.994205), defer=False)
        # have bob talk to anna
        result, status = self.send_chunk(self.bob, ['anna'])
        self.assertValidResult(result, status, 200)
        # check streams
        result, status = self.get('/v30/streams',
                                  access_token=self.bob.create_access_token())
        self.assertValidResult(result, status, 200)
        self.assertEqual(len(result['data']), 1)
        self.assertEqual(len(result['data'][0]['others']), 1)
        self.assertEqual(result['data'][0]['others'][0]['username'], 'anna')
        self.assertEqual(result['data'][0]['others'][0]['location'], 'New York')
        # now anna blocks bob
        result, status = self.post('/v30/profile/me/blocked',
                                  identifier='bob',
                                  access_token=self.anna.create_access_token())
        self.assertValidResult(result, status, 200)
        self.assertTrue(result['success'])
        # check streams again
        result, status = self.get('/v30/streams',
                                  access_token=self.bob.create_access_token())
        self.assertValidResult(result, status, 200)
        self.assertEqual(len(result['data']), 1)
        self.assertEqual(len(result['data'][0]['others']), 1)
        self.assertEqual(result['data'][0]['others'][0]['username'], 'anna')
        self.assertEqual(result['data'][0]['others'][0]['location'], None)

    def test_unblock(self):
        # block bob
        result, status = self.post('/v30/profile/me/blocked',
                                   identifier='bob',
                                   access_token=self.anna.create_access_token())
        self.assertValidResult(result, status, 200)
        self.assertTrue(result['success'])
        # Have Bob try and talk to Anna (which should fail).
        result, status = self.send_chunk(self.bob, ['anna'])
        self.assertValidResult(result, status, 400)
        # Unblock Bob.
        result, status = self.delete('/v30/profile/me/blocked/bob',
                                     access_token=self.anna.create_access_token())
        self.assertValidResult(result, status, 200)
        self.assertTrue(result['success'])
        # Have Bob try and talk to Anna again (should work now).
        result, status = self.send_chunk(self.bob, ['anna'])
        self.assertValidResult(result, status, 200)
        # Verify that a chunk was sent to Anna.
        result, status = self.get('/v30/streams/%s/chunks' % (result.get('id'),),
                                  access_token=self.anna.create_access_token())
        self.assertEqual(len(result['data']), 1)

    def test_can_see_own_active(self):
        result, status = self.get('/v30/profile/me',
                                  access_token=self.anna.create_access_token())
        self.assertValidResult(result, status, 200)
        self.assertEqual(result['status'], 'active')

    def test_reclaim_activated_identifier(self):
        # Ensure that it's not possible to add an existing identifier if it's activated.
        identifier_1, identifier_2 = ('+12345550101', '+12345550102')
        # Log in with identifier #1.
        code = self.request_code(identifier_1)
        result, status = self.post('/v30/challenge/respond', identifier=identifier_1, secret=code)
        self.assertValidResult(result, status, 200)
        account_id_1 = result['account']['id']
        access_token = result['access_token']
        # Log in with identifier #2 to make it active.
        code = self.request_code(identifier_2)
        result, status = self.post('/v30/challenge/respond', identifier=identifier_2, secret=code)
        self.assertValidResult(result, status, 200)
        account_id_2 = result['account']['id']
        self.assertNotEquals(account_id_1, account_id_2)
        # Try adding identifier_2 for the first account which
        # should return a session for the other account.
        code = self.request_code(identifier_2)
        result, status = self.post('/v30/challenge/respond', identifier=identifier_2, secret=code,
                                   access_token=access_token)
        self.assertValidResult(result, status, 200)
        self.assertEquals(account_id_2, result['account']['id'])

    def test_reclaim_nonactivated_identifier(self):
        # Ensure that it's possible to add an existing identifier if it's not activated.
        identifier_1, identifier_2 = ('+12345550101', '+12345550102')
        # Log in with identifier #1.
        code = self.request_code(identifier_1)
        result, status = self.post('/v30/challenge/respond', identifier=identifier_1, secret=code)
        self.assertValidResult(result, status, 200)
        self.assertEquals(2, len(result['account']['identifiers']))
        self.assertEquals(identifier_1, result['account']['identifiers'][0])
        # self.assertTrue(result['account']['identifiers'][1].startswith('r'))
        access_token = result['access_token']
        # Ensure that an invited account is created for identifier #2.
        self.send_chunk(self.bob, identifier_2)
        # Try adding identifier_2 for the first account.
        code = self.request_code(identifier_2)
        result, status = self.post('/v30/challenge/respond', identifier=identifier_2, secret=code,
                                   access_token=access_token)
        self.assertValidResult(result, status, 200)
        # Make sure it worked.
        self.assertIn(identifier_1, result['account']['identifiers'])
        self.assertIn(identifier_2, result['account']['identifiers'])
        self.assertEquals(3, len(result['account']['identifiers']))

    def test_default_username(self):
        identifier = '+109999999'
        code = self.request_code(identifier)

        result, status = self.post('/v30/challenge/respond', identifier=identifier, secret=code)
        self.assertValidResult(result, status, 200)
        self.assertFalse(result['account']['display_name_set'])
        # Test username is set with new account
        access_token = result['access_token']
        new_display_name = u'!1Andreas Blixt-Nörsk@123FB'
        result, status = self.post('/v30/profile/me', display_name=new_display_name,
                                   access_token=access_token)
        self.assertValidResult(result, status, 200)
        result, status = self.get('/v30/profile/me', access_token=access_token)
        self.assertValidResult(result, status, 200)
        self.assertTrue(result['display_name_set'])
        self.assertEquals(result['display_name'], new_display_name)
        self.assertEquals(result['username'], 'andreasblixtnorsk123fb')

        identifier = '+109999998'
        code = self.request_code(identifier)
        result, status = self.post('/v30/challenge/respond', identifier=identifier, secret=code)
        self.assertValidResult(result, status, 200)
        # Test username is set with new account
        access_token = result['access_token']
        new_display_name = u'!1Andreas Blixt-Nörsk@123FB'
        result, status = self.post('/v30/profile/me', display_name=new_display_name,
                                   access_token=access_token)
        self.assertValidResult(result, status, 200)
        result, status = self.get('/v30/profile/me', access_token=access_token)
        self.assertValidResult(result, status, 200)
        self.assertEquals(result['display_name'], new_display_name)
        self.assertNotEquals(result['username'], 'andreasblixtnorsk123fb')

        identifier = '+109999997'
        code = self.request_code(identifier)
        result, status = self.post('/v30/challenge/respond', identifier=identifier, secret=code)
        self.assertValidResult(result, status, 200)
        # Test username is set with new account
        access_token = result['access_token']
        new_display_name = u'1234'
        result, status = self.post('/v30/profile/me', display_name=new_display_name,
                                   access_token=access_token)
        self.assertValidResult(result, status, 200)
        result, status = self.get('/v30/profile/me', access_token=access_token)
        self.assertValidResult(result, status, 200)
        self.assertEquals(result['display_name'], new_display_name)
        self.assertNotEquals(result['username'], '')

    def test_update_username_based_on_display_name(self):
        result, status = self.post('/v30/register', display_name='')
        # self.assertTrue(result['account']['username'].startswith('r'))
        # Test username is added with new display name
        access_token = result['access_token']
        new_display_name = u'!1Andreas Blixt-Nörsk@123FB'
        result, status = self.post('/v30/profile/me', display_name=new_display_name,
                                   access_token=access_token)
        self.assertValidResult(result, status, 200)
        result, status = self.get('/v30/profile/me', access_token=access_token)
        self.assertValidResult(result, status, 200)
        self.assertEquals(result['display_name'], new_display_name)
        self.assertEquals(result['username'], 'andreasblixtnorsk123fb')

    def test_update_username_based_on_display_name_pointless(self):
        result, status = self.post('/v30/register', display_name='')
        # self.assertTrue(result['account']['username'].startswith('r'))
        # Test username is added with new display name
        access_token = result['access_token']
        new_display_name = u'!1123'
        result, status = self.post('/v30/profile/me', display_name=new_display_name,
                                   access_token=access_token)
        self.assertValidResult(result, status, 200)
        result, status = self.get('/v30/profile/me', access_token=access_token)
        self.assertValidResult(result, status, 200)
        self.assertEquals(result['display_name'], new_display_name)
        self.assertEquals(len(result['identifiers']), 1)
        # self.assertTrue(result['username'].startswith('r'))

    def test_register_update_display_name_username_update(self):
        result, status = self.post('/v30/register', display_name='')
        # self.assertTrue(result['account']['username'].startswith('r'))
        # Test username is added with new display name
        access_token = result['access_token']
        new_display_name = u'!1Andreas Blixt-Nörsk@123FB'
        result, status = self.post('/v30/profile/me', display_name=new_display_name,
                                   access_token=access_token)
        self.assertValidResult(result, status, 200)
        self.assertEquals(result['display_name'], new_display_name)
        self.assertEquals(len(result['identifiers']), 2)
        self.assertEquals(result['username'], 'andreasblixtnorsk123fb')
        new_username = u'yolo4eva'
        result, status = self.post('/v30/profile/me', username=new_username,
                                   access_token=access_token)
        self.assertValidResult(result, status, 200)
        result, status = self.get('/v30/profile/me', access_token=access_token)
        self.assertValidResult(result, status, 200)
        self.assertEquals(result['display_name'], new_display_name)
        self.assertEquals(len(result['identifiers']), 2)
        self.assertEquals(result['username'], new_username)

    def test_register_update_display_name_username_update2(self):
        result, status = self.post('/v30/register', display_name='')
        # self.assertTrue(result['account']['username'].startswith('r'))
        # Test username is added with new display name
        access_token = result['access_token']
        new_display_name = u'!1123'
        result, status = self.post('/v30/profile/me', display_name=new_display_name,
                                   access_token=access_token)
        self.assertValidResult(result, status, 200)
        self.assertEquals(result['display_name'], new_display_name)
        self.assertEquals(len(result['identifiers']), 1)
        # self.assertTrue(result['username'].startswith('r'))
        new_username = u'yolo4eva'
        result, status = self.post('/v30/profile/me', username=new_username,
                                   access_token=access_token)
        self.assertValidResult(result, status, 200)
        result, status = self.get('/v30/profile/me', access_token=access_token)
        self.assertValidResult(result, status, 200)
        self.assertEquals(result['display_name'], new_display_name)
        self.assertEquals(len(result['identifiers']), 2)
        self.assertTrue(result['username'], new_username)

    def test_register_update_username_update_display_name(self):
        result, status = self.post('/v30/register', display_name='')
        # self.assertTrue(result['account']['username'].startswith('r'))
        # Test username is added with new display name
        access_token = result['access_token']
        new_username = u'yolo4eva'
        result, status = self.post('/v30/profile/me', username=new_username,
                                   access_token=access_token)
        self.assertValidResult(result, status, 200)
        new_display_name = u'Ola ke ase'
        result, status = self.post('/v30/profile/me', display_name=new_display_name,
                                   access_token=access_token)
        self.assertValidResult(result, status, 200)
        self.assertEquals(len(result['identifiers']), 2)
        self.assertEquals(result['username'], new_username)
        result, status = self.get('/v30/profile/me', access_token=access_token)
        self.assertValidResult(result, status, 200)
        self.assertEquals(result['display_name'], new_display_name)
        self.assertEquals(len(result['identifiers']), 2)
        self.assertEquals(result['username'], new_username)

    def test_invalid_username(self):
        # "me" is reserved.
        result, status = self.post('/v30/profile/me', username='me',
                                   access_token=self.anna.create_access_token())
        self.assertValidResult(result, status, 409)
        # Ensure that the check can't be circumvented.
        result, status = self.post('/v30/profile/me', username='ME',
                                   access_token=self.anna.create_access_token())
        self.assertValidResult(result, status, 409)
        result, status = self.post('/v30/profile/me', username='me ',
                                   access_token=self.anna.create_access_token())
        self.assertValidResult(result, status, 409)
        result, status = self.post('/v30/profile/me', username='me\0',
                                   access_token=self.anna.create_access_token())
        self.assertValidResult(result, status, 400)

    def test_username_with_upper_case(self):
        result, status = self.post('/v30/profile/me', username='AnnaDalsson',
                                   access_token=self.anna.create_access_token())
        self.assertValidResult(result, status, 200)
        self.assertEqual(result.get('username'), 'annadalsson')
        # Ensure that Bob can't take the username.
        result, status = self.post('/v30/profile/me', username='ANNADALSSON',
                                   access_token=self.bob.create_access_token())
        self.assertValidResult(result, status, 409)

    def test_update_display_name(self):
        new_display_name = 'Anna Panna'
        result, status = self.post('/v30/profile/me', display_name=new_display_name,
                                   access_token=self.anna.create_access_token())
        self.assertValidResult(result, status, 200)
        result, status = self.get('/v30/profile/me',
                                  access_token=self.anna.create_access_token())
        self.assertValidResult(result, status, 200)
        self.assertIsAccount(result, self.anna)
        self.assertEquals(result['display_name'], new_display_name)

    @mock.patch('roger.files.gcs')
    def test_update_image(self, gcs_mock):
        result, status = self.post('/v30/profile/me',
                                   image=(StringIO('<image data>'), 'filename.jpg'),
                                   access_token=self.anna.create_access_token())
        self.assertValidResult(result, status, 200)
        result, status = self.get('/v30/profile/me',
                                  access_token=self.anna.create_access_token())
        self.assertValidResult(result, status, 200)
        self.assertIsAccount(result, self.anna)
        self.assertIn('image_url', result)
        self.assertEquals(result['image_url'], 'https://example.com/test.xyz')

    def test_set_username(self):
        jay = accounts.create('+12345322', status='active')
        result, status = self.get('/v30/profile/me',
                                  access_token=jay.create_access_token())
        self.assertIsAccount(result, jay)
        self.assertIsNotNone(result['display_name'])
        old_username = jay.username
        new_username = 'jay'
        result, status = self.post('/v30/profile/me', username=new_username,
                                   access_token=jay.create_access_token())
        self.assertValidResult(result, status, 200)
        # Get the profile publicly using the new username.
        result, status = self.get('/v30/profile/jay')
        self.assertValidResult(result, status, 200)
        self.assertEqual(result['id'], jay.account_id)
        self.assertEqual(result['display_name'], 'jay')
        # Check own profile again to make sure the username is there.
        result, status = self.get('/v30/profile/me',
                                  access_token=jay.create_access_token())
        self.assertValidResult(result, status, 200)
        self.assertItemsEqual([old_username, '+12345322', new_username], result['identifiers'])

    def test_update_username(self):
        new_username = 'annayolo'
        result, status = self.post('/v30/profile/me', username=new_username,
                                   access_token=self.anna.create_access_token())
        self.assertValidResult(result, status, 200)
        result, status = self.get('/v30/profile/me',
                                  access_token=self.anna.create_access_token())
        self.assertValidResult(result, status, 200)
        self.assertIn(new_username, result.get('identifiers'))
        self.assertNotIn('anna', result.get('identifiers'))

    def test_username_cannot_be_channel(self):
        # Channels can't be usernames.
        result, status = self.post('/v30/profile/me', username='#yolo',
                                   access_token=self.anna.create_access_token())
        self.assertValidResult(result, status, 400)

    def test_username_cannot_be_numeric(self):
        # Numeric usernames are not allowed.
        result, status = self.post('/v30/profile/me', username='1234567890',
                                   access_token=self.anna.create_access_token())
        self.assertValidResult(result, status, 400)

    def test_username_cannot_be_phone(self):
        # Phone numbers can't be usernames.
        result, status = self.post('/v30/profile/me', username='+123456789',
                                   access_token=self.anna.create_access_token())
        self.assertValidResult(result, status, 400)

    def test_get_profile_username(self):
        result, status = self.get('/v30/profile/anna')
        self.assertValidResult(result, status, 200)
        self.assertEqual(result.get('id'), self.anna.account_id)
        self.assertEqual(result.get('display_name'), self.anna.display_name)
        self.assertEqual(result.get('image_url'), self.anna.image_url)

    def test_get_profile_account_id(self):
        result, status = self.get('/v30/profile/%d' % self.anna.account_id)
        self.assertValidResult(result, status, 200)
        self.assertEqual(result.get('id'), self.anna.account_id)
        self.assertEqual(result.get('display_name'), self.anna.display_name)
        self.assertEqual(result.get('image_url'), self.anna.image_url)

    def test_get_profile_phone(self):
        jay = accounts.create('+12345322', status='active')
        result, status = self.get('/v30/profile/+12345322')
        self.assertValidResult(result, status, 200)
        self.assertEqual(result.get('id'), jay.account_id)
        self.assertEqual(result.get('display_name'), jay.display_name)
        self.assertEqual(result.get('image_url'), jay.image_url)

    def test_get_profile_phone_username(self):
        jay = accounts.create('+12345322', status='active')
        result, status = self.get('/v30/profile/+12345322')
        self.assertValidResult(result, status, 200)
        self.assertEqual(result.get('id'), jay.account_id)
        self.assertEqual(result.get('display_name'), jay.display_name)
        self.assertEqual(result.get('image_url'), jay.image_url)
        new_username = 'jay'
        result, status = self.post('/v30/profile/me', username=new_username,
                                   access_token=jay.create_access_token())
        result, status = self.get('/v30/profile/jay')
        self.assertValidResult(result, status, 200)
        # This should still work.
        result, status = self.get('/v30/profile/+12345322')
        self.assertValidResult(result, status, 200)
        self.assertEqual(result.get('id'), jay.account_id)

    def test_get_profile_wrong_username(self):
        result, status = self.get('/v30/profile/blixtseviltwin')
        self.assertValidResult(result, status, 404)


class Register(BaseTestCase):
    def test_birthday(self):
        result, status = self.post('/v52/register', username='snooplion', birthday='1971-10-20')
        self.assertValidResult(result, status, 200)
        self.assertEqual(result['account']['birthday'], '1971-10-20')

    def test_existing_user(self):
        # Ensure that registering an existing user gives "Conflict".
        result, status = self.post('/v30/register', username='anna', password='blah')
        self.assertValidResult(result, status, 409)
        # TODO: Ensure that nothing was reported.
        #self.assertEquals(0, self.report_mock.user_created.call_count)

    def test_gender(self):
        result, status = self.post('/v52/register', username='snoopdogg', gender='male')
        self.assertValidResult(result, status, 200)
        self.assertEqual(result['account']['gender'], 'male')

    def test_valid_username(self):
        # Ensure that a valid username gives "OK".
        result, status = self.post('/v30/register', username='bobby', password='meatball')
        self.assertValidResult(result, status, 200)
        bobby = accounts.get_handler('bobby')
        self.assertIsValidSession(result, handler=bobby, expected_status='active')
        # TODO: Ensure that the relevant events were reported.
        #self.assertEquals(1, self.report_mock.user_registered.call_count)

    def test_generate_username(self):
        result, status = self.post('/v30/register')
        self.assertValidResult(result, status, 200)
        self.assertIsNotNone(result['account'])
        self.assertIsNotNone(result['account']['username'])
        handler = accounts.get_handler(result['account']['username'])
        self.assertIsValidSession(result, handler=handler, expected_status='active', version=30)

    def test_generate_username_display_name(self):
        result, status = self.post('/v30/register', display_name='Andreas Blixt')
        self.assertValidResult(result, status, 200)
        self.assertIsNotNone(result['account'])
        self.assertIsNotNone(result['account']['username'])
        self.assertEquals('andreasblixt', result['account']['username'])
        handler = accounts.get_handler(result['account']['username'])
        self.assertIsValidSession(result, handler=handler, expected_status='active', version=30)

    def test_generate_username_display_name_empty(self):
        result, status = self.post('/v30/register', display_name='')
        self.assertValidResult(result, status, 200)
        self.assertIsNotNone(result['account'])
        self.assertIsNotNone(result['account']['username'])
        # self.assertTrue(result['account']['username'].startswith('r'))
        handler = accounts.get_handler(result['account']['username'])
        self.assertIsValidSession(result, handler=handler, expected_status='active', version=30)

    @mock.patch('roger.files.gcs')
    def test_create_account_with_image(self, gcs_mock):
        result, status = self.post('/v30/register',
                                   image=(StringIO('<image data>'), 'filename.jpg'))
        self.assertValidResult(result, status, 200)
        self.assertIsNotNone(result['account'])
        self.assertIsNotNone(result['account']['username'])
        handler = accounts.get_handler(result['account']['username'])
        self.assertEquals(result['account']['image_url'], 'https://example.com/test.xyz')
        self.assertIsValidSession(result, handler=handler, expected_status='active', version=30)


class Threads(BaseTestCase):
    def test_blocking(self):
        result, status = self.post('/v49/profile/me/blocked', identifier='cecilia',
                                  access_token=self.anna.create_access_token())
        self.assertValidResult(result, status, 200)
        result, status = self.put('/v49/threads/', identifier='anna',
                                  access_token=self.cecilia.create_access_token())
        self.assertValidResult(result, status, 404)

    def test_blocking_messaging(self):
        result, status = self.put('/v51/threads/', identifier='anna',
                                  access_token=self.bob.create_access_token())
        self.assertValidResult(result, status, 200)
        thread_id = result['id']
        result, status = self.post('/v51/threads/%s/messages/' % (thread_id,),
                                   access_token=self.bob.create_access_token(),
                                   type='text', text='Hello world!')
        self.assertValidResult(result, status, 200)
        result, status = self.post('/v51/profile/me/blocked', identifier='bob',
                                  access_token=self.anna.create_access_token())
        self.assertValidResult(result, status, 200)
        result, status = self.post('/v51/threads/%s/messages/' % (thread_id,),
                                   access_token=self.bob.create_access_token(),
                                   type='text', text='Are you there?')
        self.assertValidResult(result, status, 404)

    def test_messaging(self):
        result, status = self.put('/v49/threads/', identifier='bob',
                                  access_token=self.anna.create_access_token())
        self.assertValidResult(result, status, 200)
        thread_id = result['id']
        # Message from anna -> bob.
        result, status = self.post('/v49/threads/%s/messages/' % (thread_id,),
                                   access_token=self.anna.create_access_token(),
                                   type='text', text='Hello world!')
        self.assertValidResult(result, status, 200)
        # Check on bob's side.
        result, status = self.get('/v49/threads/%s/' % (thread_id,),
                                  access_token=self.bob.create_access_token())
        self.assertValidResult(result, status, 200)
        self.assertEqual(result['id'], thread_id)
        self.assertEqual(result['messages'][0]['text'], 'Hello world!')
        # Make sure the thread is also visible in the lists.
        result, status = self.get('/v49/threads/', access_token=self.anna.create_access_token())
        self.assertValidResult(result, status, 200)
        self.assertEqual(result['data'][0]['id'], thread_id)
        result, status = self.get('/v49/threads/', access_token=self.bob.create_access_token())
        self.assertValidResult(result, status, 200)
        self.assertEqual(result['data'][0]['id'], thread_id)

    def test_seen_until(self):
        result, status = self.put('/v50/threads/', identifier='bob',
                                  access_token=self.anna.create_access_token())
        self.assertValidResult(result, status, 200)
        thread_id = result['id']
        # Message from anna -> bob.
        result, status = self.post('/v50/threads/%s/messages/' % (thread_id,),
                                   access_token=self.anna.create_access_token(),
                                   type='text', text='Hello world!')
        self.assertValidResult(result, status, 200)
        # Check on bob's side.
        result, status = self.get('/v50/threads/%s/' % (thread_id,),
                                  access_token=self.bob.create_access_token())
        self.assertValidResult(result, status, 200)
        self.assertEqual(result['id'], thread_id)
        message = result['messages'][-1]
        self.assertIsNone(result['seen_until'])
        # Mark it as seen.
        result, status = self.post('/v50/threads/%s/' % (thread_id,),
                                   access_token=self.bob.create_access_token(),
                                   seen_until=message['id'])
        self.assertValidResult(result, status, 200)
        self.assertEqual(result['seen_until'], message['id'])


class Wallet(BaseTestCase):
    def test_pay(self):
        # Give Bob some currency.
        models.Wallet.create_and_transfer(self.bob.key, self.bob.wallet, 'b01', 30, 'Test')
        # Pay some of that currency to Cecilia.
        result, status = self.post('/v48/profile/cecilia/pay/',
                                   access_token=self.bob.create_access_token(),
                                   amount=12)
        self.assertValidResult(result, status, 200)
        # Validate balance in returned wallet.
        self.assertEqual(result['wallet']['balance'], 18)

    @mock.patch('roger.apps.api.apple')
    def test_purchase(self, apple_mock):
        apple_mock.itunes.return_value = {
            u'environment': u'Sandbox',
            u'receipt': {u'adam_id': 0,
                         u'app_item_id': 0,
                         u'application_version': u'174',
                         u'bundle_id': u'cam.reaction.ReactionCam',
                         u'download_id': 0,
                         u'in_app': [{u'is_trial_period': u'false',
                                      u'original_purchase_date': u'2017-09-20 18:33:07 Etc/GMT',
                                      u'original_purchase_date_ms': u'1505932387000',
                                      u'original_purchase_date_pst': u'2017-09-20 11:33:07 America/Los_Angeles',
                                      u'original_transaction_id': u'10001',
                                      u'product_id': u'RCOINS25',
                                      u'purchase_date': u'2017-09-20 18:33:07 Etc/GMT',
                                      u'purchase_date_ms': u'1505932387000',
                                      u'purchase_date_pst': u'2017-09-20 11:33:07 America/Los_Angeles',
                                      u'quantity': u'1',
                                      u'transaction_id': u'10001'},
                                     {u'is_trial_period': u'false',
                                      u'original_purchase_date': u'2017-09-20 18:33:07 Etc/GMT',
                                      u'original_purchase_date_ms': u'1505932387000',
                                      u'original_purchase_date_pst': u'2017-09-20 11:33:07 America/Los_Angeles',
                                      u'original_transaction_id': u'10002',
                                      u'product_id': u'RCOINS25',
                                      u'purchase_date': u'2017-09-20 18:33:07 Etc/GMT',
                                      u'purchase_date_ms': u'1505932387000',
                                      u'purchase_date_pst': u'2017-09-20 11:33:07 America/Los_Angeles',
                                      u'quantity': u'2',
                                      u'transaction_id': u'10002'},
                                     {u'is_trial_period': u'false',
                                      u'original_purchase_date': u'2017-09-20 18:33:07 Etc/GMT',
                                      u'original_purchase_date_ms': u'1505932387000',
                                      u'original_purchase_date_pst': u'2017-09-20 11:33:07 America/Los_Angeles',
                                      u'original_transaction_id': u'10003',
                                      u'product_id': u'RCOINS25',
                                      u'purchase_date': u'2017-09-20 18:33:07 Etc/GMT',
                                      u'purchase_date_ms': u'1505932387000',
                                      u'purchase_date_pst': u'2017-09-20 11:33:07 America/Los_Angeles',
                                      u'quantity': u'1',
                                      u'transaction_id': u'10003'}],
                         u'original_application_version': u'1.0',
                         u'original_purchase_date': u'2013-08-01 07:00:00 Etc/GMT',
                         u'original_purchase_date_ms': u'1375340400000',
                         u'original_purchase_date_pst': u'2013-08-01 00:00:00 America/Los_Angeles',
                         u'receipt_creation_date': u'2017-09-20 18:33:07 Etc/GMT',
                         u'receipt_creation_date_ms': u'1505932387000',
                         u'receipt_creation_date_pst': u'2017-09-20 11:33:07 America/Los_Angeles',
                         u'receipt_type': u'ProductionSandbox',
                         u'request_date': u'2017-09-20 18:48:03 Etc/GMT',
                         u'request_date_ms': u'1505933283076',
                         u'request_date_pst': u'2017-09-20 11:48:03 America/Los_Angeles',
                         u'version_external_identifier': 0},
            u'status': 0,
        }
        result, status = self.post('/v48/purchase/',
                                   access_token=self.anna.create_access_token(),
                                   receipt=base64.b64encode('Hello World'),
                                   purchase_id=['10001', '10002'])
        self.assertValidResult(result, status, 200)
        self.assertItemsEqual(result['completed_purchase_ids'], ['10001', '10002'])
        # Validate balance in returned wallet.
        self.assertEqual(result['wallet']['balance'], 75)

    def test_unlock(self):
        # Give Anna some currency.
        models.Wallet.create_and_transfer(self.anna.key, self.anna.wallet, 'a01', 5, 'Test')
        # Attempt to unlock feature with insufficient funds.
        result, status = self.post('/v55/profile/me/unlock',
                                   access_token=self.anna.create_access_token(),
                                   property='record_hq')
        self.assertValidResult(result, status, 400)
        # Make sure the account does not have the property.
        result, status = self.get('/v55/profile/me', access_token=self.anna.create_access_token())
        self.assertValidResult(result, status, 200)
        self.assertItemsEqual(result['premium_properties'], [])
        # Top up account.
        models.Wallet.create_and_transfer(self.anna.key, self.anna.wallet, 'a02', 995, 'Test')
        # Try again.
        result, status = self.post('/v55/profile/me/unlock',
                                   access_token=self.anna.create_access_token(),
                                   property='record_hq')
        self.assertValidResult(result, status, 200)
        # Make sure the account has the property.
        result, status = self.get('/v55/profile/me', access_token=self.anna.create_access_token())
        self.assertValidResult(result, status, 200)
        self.assertItemsEqual(result['premium_properties'], ['record_hq'])
