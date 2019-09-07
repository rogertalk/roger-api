from roger import accounts
import rogertests


class BaseTestCase(rogertests.RogerTestCase):
    def setUp(self):
        super(BaseTestCase, self).setUp()

        # Make sure the bots are initialized during this test.
        import roger.bots
        reload(roger.bots)

        # Create four test users.
        self.anna = accounts.create('anna', status='active')
        self.bob = accounts.create('bob')
        self.cecilia = accounts.create('cecilia')
        self.dennis = accounts.create('dennis')


class EchoBot(BaseTestCase):
    def test_echo_bot(self):
        self.anna.streams.send(['echo'], 'echome.mp3', 1000)
        recents, _ = self.anna.streams.get_recent()
        chunk = recents[0].chunks[-1]
        self.assertEqual(chunk.sender, accounts.get_handler('echo').account.key)
        self.assertEqual(chunk.payload, 'echome.mp3')
