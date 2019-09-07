import mock

from roger import auth, accounts
from roger_common import errors
import rogertests


class BaseTestCase(rogertests.RogerTestCase):
    def setUp(self):
        super(BaseTestCase, self).setUp()


class Challenge(BaseTestCase):
    @mock.patch('roger.auth.localize')
    def test_challenge_apple_review(self, localize_mock):
        # Test the contact details we give Apple for review.
        challenger = auth.get_challenger('fika', 'demo@apple.com')
        challenger.challenge()
        # Ensure we circumvented actual email sending code.
        self.assertFalse(localize_mock.send_email.called)
        # The code should always be "123456".
        self.assertTrue(challenger.validate('123456'))

    @mock.patch('roger.auth.localize')
    def test_challenge_email(self, localize_mock):
        # TODO: Change this once any email is allowed in.
        with self.assertRaises(errors.ForbiddenAction):
            auth.get_challenger('fika', 'dude@company.com').challenge()
        #self.assertTrue(localize_mock.send_email.called)

    def test_challenge_invalid_identifier(self):
        # No usernames or weird identifier allowed here.
        self.assertRaises(Exception, auth.get_challenger, 'fika', '!@')
        self.assertRaises(Exception, auth.get_challenger, 'fika', '')
        self.assertRaises(Exception, auth.get_challenger, 'fika', '+123')
        self.assertRaises(Exception, auth.get_challenger, 'fika', 'blixt')
