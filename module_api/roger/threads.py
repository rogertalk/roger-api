# -*- coding: utf-8 -*-

import functools

from google.appengine.ext import ndb

from roger import models, notifs
from roger_common import errors


class Handler(object):
    __slots__ = ['account_key']

    def __init__(self, account_key):
        if not isinstance(account_key, ndb.Key) or account_key.kind() != 'Account':
            raise TypeError('Expected an Account key')
        self.account_key = account_key

    def get_by_id(self, *args, **kwargs):
        return self.get_by_id_async(*args, **kwargs).get_result()

    @ndb.tasklet
    def get_by_id_async(self, thread_id):
        thread = yield models.Thread.get_by_id_async(thread_id)
        if not thread:
            raise errors.ResourceNotFound('Thread not found')
        raise ndb.Return(ThreadWithAccount(self.account_key, thread))

    def get_or_create(self, *args, **kwargs):
        return self.get_or_create_async(*args, **kwargs).get_result()

    @ndb.tasklet
    def get_or_create_async(self, identifier_list):
        try:
            account_keys = yield models.Account.resolve_keys_async(identifier_list)
        except:
            raise errors.InvalidArgument('Got one or more invalid identifiers')
        account_keys.add(self.account_key)
        thread = yield models.Thread.lookup_async(account_keys)
        raise ndb.Return(ThreadWithAccount(self.account_key, thread))

    def get_recent_messages(self, *args, **kwargs):
        return self.get_recent_messages_async(*args, **kwargs).get_result()

    @ndb.tasklet
    def get_recent_messages_async(self, thread_id, cursor=None, limit=50):
        key = ndb.Key('Thread', thread_id)
        q = models.ThreadMessage.query(ancestor=key)
        q = q.order(-models.ThreadMessage.created)
        q_future = q.fetch_page_async(limit, start_cursor=cursor)
        thread, (messages, next_cursor, more) = yield key.get_async(), q_future
        if not more:
            next_cursor = None
        if not thread or not any(self.account_key == a.account for a in thread.accounts):
            raise errors.ResourceNotFound('Thread not found')
        raise ndb.Return((ThreadWithAccount(self.account_key, thread), messages, next_cursor))

    def get_recent_threads(self, *args, **kwargs):
        return self.get_recent_threads_async(*args, **kwargs).get_result()

    @ndb.tasklet
    def get_recent_threads_async(self, cursor=None, limit=50):
        q = models.Thread.query()
        q = q.filter(models.Thread.visible_by == self.account_key)
        q = q.order(-models.Thread.last_interaction)
        thread_list, next_cursor, more = yield q.fetch_page_async(limit, start_cursor=cursor)
        thread_list = [ThreadWithAccount(self.account_key, t) for t in thread_list]
        raise ndb.Return((thread_list, next_cursor if more else None))

    def message(self, *args, **kwargs):
        return self.message_async(*args, **kwargs).get_result()

    @ndb.tasklet
    def message_async(self, thread_id, type, text, data, **kwargs):
        # TODO: Respect block status.
        if not isinstance(thread_id, basestring):
            raise TypeError('Expected string id')
        thread, message = yield models.Thread.add_message_async(
            ndb.Key('Thread', thread_id), self.account_key,
            type, text, data, **kwargs)
        twa = ThreadWithAccount(self.account_key, thread)
        futures = []
        for a in thread.accounts:
            hub = notifs.Hub(a.account)
            f = hub.emit_async(notifs.ON_THREAD_MESSAGE,
                thread=twa.for_account(a.account, lite=True),
                message=message)
            futures.append(f)
        yield tuple(futures)
        raise ndb.Return(twa)

    def message_identifiers(self, *args, **kwargs):
        return self.message_identifiers_async(*args, **kwargs).get_result()

    @ndb.tasklet
    def message_identifiers_async(self, identifier_list, type, text, data, **kwargs):
        thread = yield self.get_or_create_async(identifier_list)
        twa = yield self.message_async(thread.key.id(), type, text, data, **kwargs)
        raise ndb.Return(twa)


class Thread(object):
    __slots__ = ['_thread']

    def __getattr__(self, name):
        return getattr(self._thread, name)

    def __init__(self, thread):
        if not isinstance(thread, models.Thread):
            raise TypeError('Expected a Thread entity')
        self._thread = thread

    def for_account(self, account, lite=False):
        if lite:
            return ThreadWithAccountLite(account, self._thread)
        else:
            return ThreadWithAccount(account, self._thread)

    def get_account(self, account_key):
        for a in self._thread.accounts:
            if a.account == account_key:
                return a
        return None


class ThreadWithAccount(Thread):
    __slots__ = ['account_key']

    def __init__(self, account_key, thread, **kwargs):
        if not isinstance(account_key, ndb.Key) or account_key.kind() != 'Account':
            raise TypeError('Expected an Account key')
        super(ThreadWithAccount, self).__init__(thread, **kwargs)
        self.account_key = account_key

    @property
    def current(self):
        return next(a for a in self._thread.accounts if a.account == self.account_key)

    def get_lite(self):
        if isinstance(self, ThreadWithAccountLite):
            return self
        return ThreadWithAccountLite(self.account_key, self._thread)

    @ndb.transactional
    def hide(self):
        t = self._thread.key.get()
        if self.account_key not in t.visible_by:
            return
        for a in t.accounts:
            if a.account == self.account_key:
                break
        else:
            raise errors.ForbiddenAction('Cannot update that thread')
        t.visible_by.remove(self.account_key)
        t.put()
        self._thread = t

    @ndb.transactional
    def hide_for_all(self):
        # Note: Intended for admin only.
        t = self._thread.key.get()
        if not t.visible_by:
            return
        for a in t.accounts:
            if a.account == self.account_key:
                break
        else:
            raise errors.ForbiddenAction('Cannot update that thread')
        t.visible_by = []
        t.put()
        self._thread = t

    @property
    def is_seen(self):
        if not self.messages:
            return True
        seen_until = self.current.seen_until
        if seen_until is None:
            return False
        return seen_until == self.messages[0].key_with_parent(self.key)

    @property
    def others(self):
        return [a for a in self._thread.accounts if a.account != self.account_key]

    def public(self, version=None, **kwargs):
        data = {
            'id': self.key.id(),
            'created': self.created,
            'last_interaction': self.last_interaction,
            'messages': self.messages,
            'others': self.others,
        }
        if version >= 50:
            data['seen_until'] = self.current.seen_until.id() if self.current.seen_until else None
        return data

    @ndb.transactional
    def set_seen_until(self, seen_until):
        t = self._thread.key.get()
        for m in t.messages:
            if m.message_id == seen_until:
                break
        else:
            # TODO: Have another look at this logic.
            raise errors.InvalidArgument('Message id must be one of 10 most recent')
        for a in t.accounts:
            if a.account == self.account_key:
                break
        else:
            raise errors.ForbiddenAction('Cannot update that thread')
        if a.seen_until and a.seen_until.id() >= seen_until:
            return
        a.seen_until = m.key_with_parent(self.key)
        t.put()
        self._thread = t

    @ndb.transactional
    def show(self):
        t = self._thread.key.get()
        if self.account_key in t.visible_by:
            return
        for a in t.accounts:
            if a.account == self.account_key:
                break
        else:
            raise errors.ForbiddenAction('Cannot update that thread')
        t.visible_by.insert(0, self.account_key)
        t.put()
        self._thread = t


class ThreadWithAccountLite(ThreadWithAccount):
    def public(self, version=None, **kwargs):
        data = super(ThreadWithAccountLite, self).public(version=version, **kwargs)
        if version >= 50:
            del data['messages']
        return data
