# -*- coding: utf-8 -*-

import random

from roger import accounts, config


@accounts.status_handler('bot')
class BotHandler(accounts.AccountHandler):
    def on_new_chunk(self, sender, stream, chunk, mute_notification=False):
        if len(stream.participants) == 2:
            # Automatically mark new chunks as played for bot accounts.
            # XXX: This currently only happens for 1:1s with bots.
            stream.set_played_until(stream.last_chunk_end)
        # Support auto reply.
        # TODO: Merge the two stream updates.
        if self.account.autoreplies:
            reply = random.choice(self.account.autoreplies)
            stream.send(reply.source, reply.duration)


@accounts.static_handler('echo', display_name='Echo')
class Echo(BotHandler):
    """Sends back whatever you send to it."""

    def on_new_chunk(self, sender, stream, chunk, mute_notification=False):
        stream.set_played_until(chunk.end)
        stream.send(chunk.payload, chunk.duration)


@accounts.static_handler('greeting', display_name='Greeting Updater')
class GreetingSetter(BotHandler):
    """Updates the user's greeting message."""

    def on_new_chunk(self, sender, stream, chunk, mute_notification=False):
        stream.set_played_until(chunk.end)
        sender.set_greeting(chunk.payload, chunk.duration)
