import mock

from roger import accounts, streams
from roger_common import errors
import rogertests


class BaseTestCase(rogertests.RogerTestCase):
    def setUp(self):
        super(BaseTestCase, self).setUp()

        # Make sure the bots are initialized during this test.
        import roger.bots
        reload(roger.bots)


class Creation(BaseTestCase):
    def setUp(self):
        super(Creation, self).setUp()
        self.ricardo = accounts.create('ricardovice', status='active')
        self.blixt = accounts.create('blixt', status='active')

    def test_add_identifier(self):
        # Don't allow changing identifier to something that's taken.
        with self.assertRaises(errors.AlreadyExists):
            self.blixt.add_identifier('ricardovice')

        self.assertItemsEqual(self.blixt.identifiers, ['blixt'])

        self.blixt.add_identifier('ab')
        # Validate that the account model was updated.
        self.assertItemsEqual(self.blixt.identifiers, ['ab', 'blixt'])
        # Also validate the data in the datastore.
        self.assertItemsEqual(accounts.get_handler('blixt').identifiers, ['ab', 'blixt'])

        # Ensure that "ab" is now unavailable.
        with self.assertRaises(errors.AlreadyExists):
            accounts.create('ab')

    def test_creation(self):
        bob = accounts.create('bob', status='active')
        self.assertEqual(bob.identifiers, ['bob'])

        # Don't allow creation of an existing account.
        with self.assertRaises(errors.AlreadyExists):
            accounts.create('bob')

    def test_change_identifier(self):
        # Don't allow changing identifier to something that's taken.
        with self.assertRaises(errors.AlreadyExists):
            self.blixt.change_identifier('blixt', 'ricardovice')

        # Don't allow changing identifier that you don't own.
        with self.assertRaises(errors.ForbiddenAction):
            self.blixt.change_identifier('ricardovice', 'mfdoom')

        # This shouldn't do anything, but make sure it doesn't cause errors.
        self.blixt.change_identifier('blixt', 'blixt')

        self.assertEqual(self.blixt.identifiers, ['blixt'])

        self.blixt.change_identifier('blixt', 'ab')
        self.assertEqual(self.blixt.identifiers, ['ab'])

        # Ensure that "ab" is now unavailable.
        with self.assertRaises(errors.AlreadyExists):
            accounts.create('ab')

        # Ensure that it's possible to change back.
        self.blixt.change_identifier('ab', 'blixt')
        self.assertEqual(self.blixt.identifiers, ['blixt'])

        # Ensure that it's possible to use the now freed up identifier.
        ab = accounts.create('ab')
        self.assertEqual(ab.identifiers, ['ab'])

    def test_remove_identifier(self):
        zandra = accounts.create('zandra', status='active')
        # Don't allow removing someone else's identifier.
        with self.assertRaises(errors.ForbiddenAction):
            self.blixt.remove_identifier('zandra')
        # Don't allow removing the last identifier.
        with self.assertRaises(errors.ForbiddenAction):
            zandra.remove_identifier('zandra')
        # Add and remove an identifier.
        zandra.add_identifier('alexandra')
        self.assertItemsEqual(zandra.identifiers, ['alexandra', 'zandra'])
        zandra.remove_identifier('zandra')
        self.assertItemsEqual(zandra.identifiers, ['alexandra'])
        # Ensure that the identifier is available again.
        zandra_2 = accounts.create('zandra', status='active')
        self.assertItemsEqual(zandra_2.identifiers, ['zandra'])
        # Ensure that both identifiers are now unavailable.
        with self.assertRaises(errors.AlreadyExists):
            zandra.change_identifier('alexandra', 'zandra')
        with self.assertRaises(errors.AlreadyExists):
            zandra_2.change_identifier('zandra', 'alexandra')


class Identifiers(BaseTestCase):
    def test_brazil_number(self):
        # Brazil has a special rule where a phone number can have two variants.
        # Using a legacy number:
        bruna = accounts.create('bruna', status='active')
        bruna.add_identifier('+554467891234')
        self.assertItemsEqual(bruna.identifiers, ['bruna', '+554467891234', '+5544967891234'])
        # Using a converted number:
        karyna = accounts.create('karyna', status='active')
        karyna.add_identifier('+5522967891234')
        self.assertItemsEqual(karyna.identifiers, ['karyna', '+552267891234', '+5522967891234'])
        # Using a landline number (no additional number):
        jully = accounts.create('jully', status='active')
        jully.add_identifier('+552233891234')
        self.assertItemsEqual(jully.identifiers, ['jully', '+552233891234'])
        # Using a new mobile number (no legacy equivalent):
        molinna = accounts.create('molinna', status='active')
        molinna.add_identifier('+5522947891234')
        self.assertItemsEqual(molinna.identifiers, ['molinna', '+5522947891234'])
        # Takeover when temporary account has the legacy number:
        anonymous = accounts.create('+554477881234', status='temporary')
        pedro = accounts.create('pedro', status='active')
        pedro.add_identifier('+5544977881234')
        self.assertItemsEqual(pedro.identifiers, ['pedro', '+5544977881234', '+554477881234'])


class PasswordValidation(BaseTestCase):
    def test_password_validation(self):
        # Verify that accounts can set passwords.
        blixt = accounts.create('blixt')
        blixt.set_password('pa$$word')

        # Verify that the password can be used to log in.
        self.assertTrue(blixt.validate_password('pa$$word'))

        # Ensure that an incorrect password does not work.
        self.assertFalse(blixt.validate_password('invalid!'))

        # Change the password.
        blixt.set_password('new passw0rd')

        # Make sure the old one doesn't work.
        self.assertFalse(blixt.validate_password('pa$$word'))

        # But the new one should work.
        self.assertTrue(blixt.validate_password('new passw0rd'))


class StaticHandlers(BaseTestCase):
    def setUp(self):
        super(StaticHandlers, self).setUp()

        @accounts.static_handler('hal9000')
        class HAL9000(accounts.AccountHandler):
            pass
        self.hal9000_class = HAL9000

        # Reserve an account for the "skynet" handler.
        self.skynet = accounts.create('skynet')
        self.skynet.add_identifier('+12345678')

        @accounts.static_handler('skynet')
        class Skynet(accounts.AccountHandler):
            pass
        self.skynet_class = Skynet

    def tearDown(self):
        super(StaticHandlers, self).tearDown()
        accounts.unregister_static_handler('hal9000')
        accounts.unregister_static_handler('skynet')

    def test_static_handler(self):
        handler = accounts.get_handler('hal9000')
        # Ensure that we got the correct type of class.
        self.assertIsInstance(handler, self.hal9000_class)
        # Ensure that there is a "handler" property on the class pointing to the handler.
        self.assertEqual(self.hal9000_class.handler, handler)

    def test_static_handler_with_account(self):
        # Ensure that the handler is found explicitly.
        handler = accounts.get_handler('skynet')
        self.assertIsInstance(handler, self.skynet_class)
        # Ensure that the handler is found via an identifier.
        handler = accounts.get_handler('+12345678')
        self.assertIsInstance(handler, self.skynet_class)
        # Ensure that the handler can be found via the account's key.
        handler = accounts.get_handler(self.skynet.account.key)
        self.assertIsInstance(handler, self.skynet_class)
        # Ensure that the handler can also be found via the account id.
        handler = accounts.get_handler(self.skynet.account_id)
        self.assertIsInstance(handler, self.skynet_class)


class Status(BaseTestCase):
    def setUp(self):
        super(Status, self).setUp()
        self.activations = 0
        def incr(account):
            self.activations += 1
        accounts.activation_hooks['test_activation'] = incr

    def tearDown(self):
        super(Status, self).tearDown()
        del accounts.activation_hooks['test_activation']

    def test_activation_trigger_runs_once(self):
        bob = accounts.create('bob')
        self.assertEqual(self.activations, 0)
        bob.change_status('active')
        self.assertEqual(self.activations, 1)
        bob.change_status('inactive')
        self.assertEqual(bob.status, 'inactive')
        bob.change_status('active')
        self.assertEqual(self.activations, 1)

    def test_change_status(self):
        ricardo = accounts.create('ricardovice', status='requested')
        self.assertEqual(ricardo.account.status, 'requested')
        ricardo.change_status('inactive')
        self.assertEqual(ricardo.account.status, 'inactive')
        # Status must be valid.
        with self.assertRaises(errors.InvalidArgument):
            ricardo.change_status('yoloing')
        with self.assertRaises(errors.InvalidArgument):
            ricardo.change_status(None)
        # Status may not be changed back to temporary etc.
        with self.assertRaises(errors.ForbiddenAction):
            ricardo.change_status('temporary')
        self.assertEqual(ricardo.account.status, 'inactive')

    def test_default(self):
        bob = accounts.create('bob')
        self.assertEqual(bob.account.status, 'temporary')
        self.assertEqual(bob.identifiers, ['bob'])

    def test_logging_in_activates(self):
        arthur = accounts.create('arthur', status='invited')
        arthur.create_session()
        self.assertEqual(arthur.status, 'active')
        self.assertEqual(self.activations, 1)

    def test_logging_in_becomes_active(self):
        don = accounts.create('don', status='inactive')
        don.create_session()
        self.assertEqual(don.status, 'active')
        self.assertEqual(self.activations, 0)

    def test_specific(self):
        # Try specifying a status.
        adam = accounts.create('adam', status='invited')
        self.assertEqual(adam.account.status, 'invited')
        self.assertEqual(adam.identifiers, ['adam'])
