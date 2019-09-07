# -*- coding: utf-8 -*-

import json
import logging
import urllib

from google.appengine.api import urlfetch
from google.appengine.ext import deferred, ndb

from roger import config, models


def _api(*args, **kwargs):
    async = kwargs.pop('_async', False)
    pending = _api_async(*args, **kwargs)
    return pending if async else pending.get_result()


@ndb.tasklet
def _api_async(*args, **kwargs):
    kwargs.setdefault('deadline', 60)
    context = ndb.get_context()
    result = yield context.urlfetch(*args, **kwargs)
    if not 200 <= result.status_code < 300:
        logging.warning('HTTP %d:\n%s', result.status_code, result.content)
        raise ndb.Return(None)
    try:
        data = json.loads(result.content)
    except:
        logging.error('Invalid JSON data:\n%s', result.content)
        raise ndb.Return(None)
    if not data['ok']:
        logging.warning('Slack API request failed:\n%s', data)
        raise ndb.Return(None)
    raise ndb.Return(data)


def _message(payload):
    return _message_async(payload).get_result()


@ndb.tasklet
def _message_async(payload):
    hook_id = payload.pop('hook_id', 'roger')
    url = config.SLACK_WEBHOOK_URLS.get(hook_id)
    if not url:
        logging.error('Invalid Slack web hook: %r', hook_id)
        return
    if config.DEVELOPMENT:
        logging.debug('Slack web hook: %r', payload)
        return
    context = ndb.get_context()
    try:
        yield context.urlfetch(method=urlfetch.POST, url=url, payload=json.dumps(payload))
    except urlfetch.DeadlineExceededError:
        logging.warning('Deadline exceeded for Slack request')


def admin(v, label=None):
    url = 'https://api.reaction.cam/admin'
    if isinstance(v, models.Account):
        url += '/accounts/%d/' % (v.key.id())
        if not label:
            label = v.display_name or v.username
    if isinstance(v, models.Content):
        url += '/content/%d/' % (v.key.id())
        if not label:
            label = v.title or '(Untitled)'
    if isinstance(v, models.ServiceTeam):
        url += '/services/%s/teams/%s' % (v.key.parent().id(), v.key.id())
        if not label:
            label = v.name
    if label:
        result = '<%s|%s>' % (url, label)
    else:
        result = '<%s>' % (url,)
    return '*%s*' % (result,)


def attachment(fallback, **kwargs):
    kwargs.setdefault('fallback', fallback)
    return kwargs


def chat(channel, text, access_token, _async=False, **kwargs):
    qs = urllib.urlencode({'channel': channel, 'token': access_token})
    url = 'https://slack.com/api/chat.postMessage?%s' % (qs,)
    for k, v in kwargs.iteritems():
        if not isinstance(v, (dict, list)):
            continue
        kwargs[k] = json.dumps(v)
    kwargs['text'] = text.encode('utf-8')
    payload = urllib.urlencode(kwargs)
    return _api(
        url, _async=_async, payload=payload, method='POST',
        headers={'Content-Type': 'application/x-www-form-urlencoded'})


def exchange_code(code, _async=False, **kwargs):
    args = {
        'client_id': config.SLACK_CLIENT_ID,
        'client_secret': config.SLACK_CLIENT_SECRET,
        'code': code,
    }
    args.update(kwargs)
    url = 'https://slack.com/api/oauth.access?' + urllib.urlencode(args)
    return _api(url, _async=_async)


def field(title, value):
    return dict(title=title, value=value)


def get_channel_info(channel, access_token, _async=False):
    url = 'https://slack.com/api/channels.info?channel=%s&token=%s&exclude_archived=1' % (
        channel, access_token)
    return _api(url, _async=_async)


def get_channel_list(access_token, _async=False):
    url = 'https://slack.com/api/channels.list?token=%s&exclude_archived=1' % (
        access_token,)
    return _api(url, _async=_async)


def get_group_info(channel, access_token, _async=False):
    url = 'https://slack.com/api/groups.info?channel=%s&token=%s&exclude_archived=1' % (
        channel, access_token)
    return _api(url, _async=_async)


def get_group_list(access_token, _async=False):
    url = 'https://slack.com/api/groups.list?token=%s&exclude_archived=1' % (
        access_token,)
    return _api(url, _async=_async)


def get_team_info(access_token, _async=False):
    return _api('https://slack.com/api/team.info?token=%s' % (access_token,),
                _async=_async)


def get_user_info(user_id, access_token, _async=False):
    url = 'https://slack.com/api/users.info?token=%s&user=%s' % (
        access_token, user_id)
    return _api(url, _async=_async)


def get_users(access_token, _async=False):
    url = 'https://slack.com/api/users.list?token=%s' % (access_token,)
    return _api(url, _async=_async)


def message(**kwargs):
    # See this page for an explanation of supported arguments:
    # https://api.slack.com/incoming-webhooks
    defer = kwargs.pop('defer', True)
    if defer:
        deferred.defer(_message, kwargs)
    else:
        _message(kwargs)


def message_async(**kwargs):
    return _message_async(kwargs)


def short_field(title, value):
    return dict(title=title, value=value, short='true')
