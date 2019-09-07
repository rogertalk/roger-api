import mock

from roger import ratelimit
import rogertests


class BaseTestCase(rogertests.RogerTestCase):
    def setUp(self):
        super(BaseTestCase, self).setUp()


class RateLimit(BaseTestCase):
    def test_default_limit(self):
        times = 0
        for i in xrange(105):
            times += 1 if ratelimit.spend() else 0
        # We should be able to spend 100 tokens by default.
        self.assertEqual(times, 100)
