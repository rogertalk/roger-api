import json
import types
import unittest

from google.appengine.datastore import datastore_stub_util
from google.appengine.ext import ndb, testbed


def load_tests(loader, tests, pattern):
    return loader.discover('.')


class RogerTestCase(unittest.TestCase):
    def setUp(self):
        self.policy = datastore_stub_util.PseudoRandomHRConsistencyPolicy(probability=1)
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_app_identity_stub()
        self.testbed.init_channel_stub()
        self.testbed.init_datastore_v3_stub(consistency_policy=self.policy)
        self.testbed.init_memcache_stub()
        self.memcache_stub = self.testbed.get_stub(testbed.MEMCACHE_SERVICE_NAME)
        self.testbed.init_taskqueue_stub(root_path='../')
        self.taskqueue_stub = self.testbed.get_stub(testbed.TASKQUEUE_SERVICE_NAME)
        self.testbed.init_urlfetch_stub()
        # Allow tests to handle urlfetch requests.
        urlfetch = self.testbed.get_stub('urlfetch')
        test = self
        def forward_fetch(self, request, response):
            test.handle_urlfetch(request, response)
        urlfetch._Dynamic_Fetch = types.MethodType(forward_fetch, urlfetch)
        # Reset caches.
        ndb.get_context().clear_cache()
        self.clear_memcache()

    def tearDown(self):
        self.testbed.deactivate()

    def clear_memcache(self):
        self.memcache_stub._the_cache = {}

    def flush_taskqueue(self, queue='default'):
        return self.taskqueue_stub.GetTasks(queue)

    def handle_urlfetch(self, request, response):
        # Just make all requests fail with a 501 error.
        response.set_content('{}')
        response.set_statuscode(501)


class JSONServiceTestCase(RogerTestCase):
    def assertIsAccount(self, data, handler, version=30):
        self.assertIsInstance(data, dict)
        self.assertEqual(data.get('id'), handler.account_id)
        for identifier in data['identifiers']:
            self.assertTrue(handler.has_identifier(identifier))
        self.assertIsInstance(data.get('display_name'), basestring)

    def assertValidResult(self, result, code, expected_code):
        self.assertIsInstance(result, dict, 'API must respond with a JSON object')
        # Get an error message to decorate mismatching error codes with.
        if isinstance(result, dict):
            message = result.get('error', {}).get('message')
        else:
            message = None
        self.assertEqual(code, expected_code, message)
        # Perform success/error specific assertions.
        if code == 200:
            # To avoid a confusing API, we enforce that "data" is always a list.
            if 'data' in result:
                self.assertIsInstance(result['data'], list,
                                      '"data" property must always be a list')
        elif code >= 400 and code < 600:
            # For client/server errors, expect there to be a valid error object.
            self.assertIsInstance(result.get('error'), dict)
            self.assertIsInstance(result['error'].get('code'), (int, long))
            self.assertIsInstance(result['error'].get('message'), basestring)
            self.assertIsInstance(result['error'].get('type'), basestring)

    def delete(self, path, **kwargs):
        return self.open('DELETE', path, **kwargs)

    def get(self, path, **kwargs):
        return self.open('GET', path, **kwargs)

    def post(self, path, **kwargs):
        return self.open('POST', path, **kwargs)

    def put(self, path, **kwargs):
        return self.open('PUT', path, **kwargs)

    @ndb.toplevel
    def open(self, method, path, **kwargs):
        assert self.client, 'self.client must be initialized'

        headers = {'User-Agent': kwargs.pop('user_agent', 'ReactionCam/123 VoiceOver/0')}
        if 'access_token' in kwargs:
            headers['Authorization'] = 'Bearer %s' % (kwargs.pop('access_token'),)

        if method == 'POST':
            data = kwargs
            query_string = None
        else:
            data = None
            query_string = kwargs

        response = self.client.open(path, query_string=query_string, method=method,
                                    headers=headers, data=data)
        return json.loads(response.data), response.status_code

    def setUp(self):
        super(JSONServiceTestCase, self).setUp()
        self.client = None
        self.longMessage = True


import test_accounts
import test_api
import test_auth
import test_bots
import test_identifiers
import test_localize
import test_oauth
import test_ratelimit
import test_report
import test_streams
import test_wallet
