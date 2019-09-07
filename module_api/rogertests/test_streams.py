import unittest

from google.appengine.ext import db

from roger import accounts, streams
import rogertests


class BaseTestCase(rogertests.RogerTestCase):
    def setUp(self):
        super(BaseTestCase, self).setUp()

        # Make sure the bots are initialized during this test.
        import roger.bots
        reload(roger.bots)

        # Create four test users.
        self.anna = accounts.create('anna', status='active')
        self.bob = accounts.create('bob', status='active')
        self.cecilia = accounts.create('cecilia', status='active')
        self.dennis = accounts.create('dennis', status='active')


class Streams(BaseTestCase):
    def test_adding_participant(self):
        # First, create and validate a stream.
        self.bob.streams.send(['anna'], 'bob1.mp3', 1000)
        recents, _ = self.anna.streams.get_recent()
        stream = recents[0]
        self.assertEqual(self.bob.account.key, stream.chunks[-1].sender)
        anna_joined = stream.joined

        # Add a third participant and validate that the stream was updated.
        stream.invite('cecilia')
        self.assertEqual(len(stream.participants), 3)

        # Ensure that it can be found through the index.
        found_stream = self.anna.streams.get(['bob', 'cecilia'])
        self.assertIsNotNone(found_stream)
        self.assertEqual(found_stream.key, stream.key)
        self.assertEqual(found_stream.last_chunk_end, stream.last_chunk_end)

        # Validate that Cecilia receives chunks on the stream.
        found_stream.send('anna1.mp3', 1000)
        recents, _ = self.cecilia.streams.get_recent()
        cecilia_stream = recents[0]
        self.assertEqual(cecilia_stream.key, stream.key)
        self.assertEqual(self.anna.account.key, cecilia_stream.chunks[-1].sender)

        # Make sure that the old participant's state remained the same.
        self.assertEqual(found_stream.joined, anna_joined)

    def test_adding_many_participants(self):
        stream = self.anna.streams.get_or_create([], title='BigGroup')
        for i in xrange(30):
            account = accounts.create(status='active')
            try:
                stream.invite(account.key)
            except db.BadRequestError:
                self.fail('adding participant #%d failed' % (i + 2,))
        self.assertEqual(len(stream.participants), 31)

    def test_loading_all_chunks(self):
        stream = self.anna.streams.get_or_create(['bob'])
        for i in range(20):
            stream.send('chunk{}.mp3'.format(i), 1000)
        stream = self.bob.streams.get(['anna'], all_chunks=True)
        self.assertEqual(len(stream.chunks), 20)

    def test_mark_older_played(self):
        stream = self.anna.streams.get_or_create(['bob'])
        stream.send('chunk1.mp3', 1000)
        stream.send('chunk2.mp3', 1000)
        stream.send('chunk3.mp3', 1000, mark_older_played=True)
        stream = self.bob.streams.get(['anna'])
        self.assertEqual(len(stream.chunks), 3)
        self.assertFalse(stream.is_played)
        self.assertEqual(stream.chunks[1].end, stream.played_until)

    def test_removing_participant(self):
        # First, create and validate a stream.
        self.cecilia.streams.send(['bob'], 'cecilia1.mp3', 1000)
        recents, _ = self.cecilia.streams.get_recent()
        stream = recents[0]
        self.assertEqual(self.cecilia.account.key, stream.chunks[-1].sender)
        cecilia_played_until = stream.played_until

        # Add two more participants.
        stream.invite(['anna', 'dennis'])
        self.assertEqual(len(stream.participants), 4)

        # Ensure that it can be found through the index.
        found_stream = self.bob.streams.get(['anna', 'cecilia', 'dennis'])
        self.assertIsNotNone(found_stream)
        self.assertEqual(found_stream.key, stream.key)

        # Make Bob leave the stream.
        found_stream.leave()

        # Ensure that the stream cannot be found with old indexes.
        no_stream = self.bob.streams.get(['cecilia'])
        self.assertIsNone(no_stream)
        no_stream = self.bob.streams.get(['anna', 'cecilia', 'dennis'])
        self.assertIsNone(no_stream)

        # Validate the new index and that Cecilia receives chunks on the stream.
        found_stream = self.dennis.streams.send(['anna', 'cecilia'], 'dennis1.mp3', 1000)
        self.assertIsNotNone(found_stream)
        self.assertEqual(found_stream.key, stream.key)
        recents, _ = self.cecilia.streams.get_recent()
        cecilia_stream = recents[0]
        self.assertEqual(cecilia_stream.key, stream.key)
        self.assertEqual(self.dennis.account.key, cecilia_stream.chunks[-1].sender)

        # Make sure that the old participant's state remained the same.
        self.assertEqual(cecilia_stream.played_until, cecilia_played_until)

    def test_removing_last_participant(self):
        # Create a stream and then leave it.
        stream = self.anna.streams.get_or_create([], title='ByeBye')
        stream_id = stream.key.id()
        stream.leave()
        # Ensure that the index is not still there.
        stream = self.anna.streams.get_or_create([], title='ByeBye')
        self.assertNotEqual(stream.key.id(), stream_id)

    def test_joining_empty_stream(self):
        # Create a shareable stream and leave it.
        stream = self.anna.streams.get_or_create([], shareable=True, title='Hey')
        stream_id = stream.key.id()
        invite_token = stream.invite_token
        stream.leave()
        # Join the stream as Bob.
        stream = streams.get_by_invite_token(invite_token).join(self.bob.account)
        self.assertEqual(len(stream.participants), 1)

    def test_legacy_index_upgrade(self):
        stream = self.anna.streams.get_or_create(['bob', 'cecilia'])
        # Force clear the index of the stream to simulate a legacy stream.
        entity = stream._stream
        self.assertIsNotNone(entity.index)
        entity.index = None
        entity.put()
        # Send something to the stream which should upgrade the stream's index.
        stream.send('test1.mp3', 1000)
        # Get a fresh copy of the stream.
        new_stream = self.anna.streams.get_by_id(entity.key.id())
        # Ensure that the index has been rebuilt.
        self.assertIsNotNone(new_stream._stream.index)

    def test_sending(self):
        # Send a chunk to a user and verify that it's at the top of their list.
        self.dennis.streams.send(['cecilia'], 'dennis1.mp3', 1000)

        # Validate the state of the receiver's list.
        recents, _ = self.cecilia.streams.get_recent()
        self.assertEqual(len(recents), 1)  # There should be only Dennis
        stream = recents[0]
        self.assertEqual(self.dennis.account.key, stream.chunks[-1].sender)
        self.assertFalse(stream.is_played)
        self.assertEqual(stream.chunks[-1].payload, 'dennis1.mp3')

        # Mark the stream as played and validate state.
        stream.set_played_until(stream.last_chunk_end)
        recents, _ = self.cecilia.streams.get_recent()
        self.assertTrue(recents[0].is_played)

        # Send another message to validate that played flag is reset.
        self.dennis.streams.send(['cecilia'], 'dennis2.mp3', 1000)
        recents, _ = self.cecilia.streams.get_recent()
        stream = recents[0]
        self.assertFalse(stream.is_played)
        self.assertEqual(stream.chunks[-1].payload, 'dennis2.mp3')

    def test_receiving_change_status(self):
        cecilia = self.cecilia
        self.assertEqual(cecilia.account.status, 'active')
        self.dennis.streams.send(['cecilia'], 'dennis3.mp3', 1000)
        self.assertEqual(cecilia.account.status, 'active')

        elin = accounts.create('elin')
        self.assertEqual(elin.account.status, 'temporary')
        self.dennis.streams.send(['elin'], 'dennis4.mp3', 1000)
        elin.load()
        self.assertEqual(elin.account.status, 'invited')
