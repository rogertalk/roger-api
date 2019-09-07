# -*- coding: utf-8 -*-

from google.appengine.api import taskqueue

from roger import config
from roger_common import bigquery_api, events, random


class BatchedBigQueryReporter(object):
    def report_async(self, event):
        row = vars(event)
        # Create an insert id for the event so that double runs won't insert it twice.
        row[bigquery_api.INSERT_ID_KEY] = random.base62(10)
        task = taskqueue.Task(
            method='PULL',
            tag=event.name,
            payload=bigquery_api.json_encoder.encode(row))
        return task.add_async(queue_name=config.BIGQUERY_QUEUE_NAME)


def account_activated(identifier, previous_status, reason, **kwargs):
    e = events.AccountActivatedV2(identifier, previous_status=previous_status,
                                  reason=reason, **kwargs)
    e.report()


def challenge_request(identifier, challenge, **kwargs):
    e = events.ChallengeV1(identifier, auth_identifier=identifier,
                           challenge=challenge, step='request', **kwargs)
    e.report()


def invite(inviter, invited, **kwargs):
    e = events.InviteV1(inviter, invited_identifier=invited, **kwargs)
    e.report()


def user_logged_in(identifier, auth_identifier, challenge, **kwargs):
    e = events.ChallengeV1(identifier, auth_identifier=auth_identifier,
                           challenge=challenge, step='success', **kwargs)
    e.report()


def user_login_failed(identifier, challenge, **kwargs):
    e = events.ChallengeV1(identifier, auth_identifier=identifier,
                           challenge=challenge, step='failed', **kwargs)
    e.report()


def user_registered(identifier, auth_identifier, challenge, status, **kwargs):
    e = events.NewAccountV1(identifier, auth_identifier=auth_identifier,
                            challenge=challenge, status=status, **kwargs)
    e.report()
