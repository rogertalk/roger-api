# -*- coding: utf-8 -*-

import flask
import mock
import unittest

from roger import accounts, report
from roger_common import events, reporting
import rogertests


class TestReporter(object):
    def __init__(self):
        self.reset()

    def report(self, event):
        self.last_event = event

    def reset(self):
        self.last_event = None


class BaseTestCase(rogertests.RogerTestCase):
    def setUp(self):
        super(BaseTestCase, self).setUp()

        # Make sure the bots are initialized during this test.
        import roger.bots
        reload(roger.bots)

        # Set up a few accounts to use for testing.
        self.anna = accounts.create('anna', status='active')
        self.bob = accounts.create('bob', status='active')

        self.app = flask.Flask(__name__)


class Event(BaseTestCase):
    def setUp(self):
        super(Event, self).setUp()
        self.reporter = TestReporter()
        reporting.add_reporter(self.reporter)

    def tearDown(self):
        super(Event, self).tearDown()
        reporting.remove_reporter(self.reporter)

    def assertValidEvent(self, event_type, identifier, **kwargs):
        event = self.reporter.last_event
        self.assertIsInstance(event, event_type)
        self.assertEquals(reporting.Identifier.anonymize(identifier), event.identifier)
        for key, value in kwargs.iteritems():
            self.assertEquals(value, getattr(event, key))
        self.reporter.reset()

    def test_challenge_request(self):
        number = '+1800123'
        with self.app.test_request_context():
            report.challenge_request(number, challenge='snailmail')
        self.assertValidEvent(events.ChallengeV1, number,
                              auth_identifier=reporting.Identifier.anonymize(number),
                              challenge='snailmail', step='request')

    def test_operator_with_unicode(self):
        values = dict(
            operator_name=u'中華電信',
            mcc='123',
            mnc='456')
        with self.app.test_request_context():
            event = events.OperatorV1('bob', **values)
            event.report()
        self.assertValidEvent(events.OperatorV1, 'bob', **values)

    def test_user_registered(self):
        email = 'a@b.c'
        with self.app.test_request_context():
            report.user_registered(self.anna.account_id, auth_identifier=email,
                                   challenge='fingerprint', status='yoloing')
        self.assertValidEvent(events.NewAccountV1, self.anna.account_id,
                              auth_identifier=reporting.Identifier.anonymize(email),
                              challenge='fingerprint')

    def test_user_logged_in(self):
        with self.app.test_request_context():
            report.user_logged_in(self.anna.account_id, auth_identifier=None,
                                  challenge='password')
        self.assertValidEvent(events.ChallengeV1, self.anna.account_id,
                              challenge='password', step='success')

    def test_user_login_failed(self):
        number = '+1800123'
        with self.app.test_request_context():
            report.user_login_failed(number, challenge='password')
        self.assertValidEvent(events.ChallengeV1, number,
                              auth_identifier=reporting.Identifier.anonymize(number),
                              challenge='password', step='failed')
