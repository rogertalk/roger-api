# -*- coding: utf-8 -*-

import json
import logging
import re
import urllib

from google.appengine.api import taskqueue, urlfetch
from google.appengine.ext import ndb

from flask import Flask, g, make_response, request

from roger import accounts, apps, auth, config, models, slack_api
from roger_common import convert, errors, flask_extras, security


app = Flask(__name__)
apps.set_up(app)


RESPONSE_TEMPLATE = """<html>
<head>
<title>Please wait...</title>
<meta http-equiv="refresh" content="0; url=%(uri)s">
<style>
a { text-decoration: none; }
body {
    align-items: center;
    display: flex;
    flex-direction: column;
    font-family: -apple-system, BlinkMacSystemFont;
    font-size: 2vh;
    height: 100vh;
    justify-content: center;
    margin: 0;
}
</style>
</head>
<body>
<p><a href="%(uri)s">Tap here if nothing happens.</a></p>
<script>
var a = document.querySelector('a'), t = a.textContent, i = 0;
a.textContent = '';
setInterval(function () { a.textContent = i < 50 ? (i++ %% 3 ? a.textContent + '.' : '.') : t }, 300);
window.location = '%(uri)s';
</script>
</body>
</html>
"""


def _exchange_code(client, code, account=None, **kwargs):
    # Fetch Slack session info with the Slack authorization code.
    auth = slack_api.exchange_code(code, **kwargs)
    if not auth:
        raise ValueError('Invalid code')
    # Try to put together the two very different responses from Slack.
    team_properties = {}
    if 'user' in auth and 'team' in auth:
        team_id = auth['team']['id']
        team_properties['name'] = auth['team']['name']
        team_properties['slug'] = auth['team']['domain']
        user_id = auth['user']['id']
        # Warning: This is real_name, *not* same as name in users.info.
        name = auth['user']['name']
        email = auth['user']['email']
        # This endpoint doesn't provide image_original.
        for key in ['image_512', 'image_192']:
            if key in auth['user']:
                image = auth['user'][key]
                break
        else:
            image = None
    else:
        # TODO: Potentially import team domain from auth.test endpoint?
        team_id = auth['team_id']
        team_properties['name'] = auth['team_name']
        user_id = auth['user_id']
        info = slack_api.get_user_info(user_id, auth['access_token'])
        if not info:
            raise ValueError('Failed to get Slack user info')
        name = info['user']['real_name'] or info['user']['name']
        email = info['user']['profile']['email']
        image = info['user']['profile'].get('image_original')
    # Use the Slack user id to lookup or create a user.
    identifier = models.Service.build_identifier('slack', team_id, user_id)
    # Attempt to get an existing account by either email or Slack identifier.
    # TODO: Use transaction?
    if account:
        handler = accounts.get_handler(account)
        if not handler.has_identifier(identifier):
            logging.debug('Adding Slack account to existing account')
            handler.add_identifier(identifier, notify_change=False)
        if not handler.has_identifier(email):
            try:
                logging.debug('Adding %s to existing account', email)
                handler.add_identifier(email, notify_change=False, notify_connect=False)
            except errors.AlreadyExists:
                logging.warning('%s already belongs to another account', email)
    else:
        ids = [identifier, email]
        handler = accounts.get_or_create(*ids, display_name=name, image=image)
    # Connect the user to the Slack team.
    auth = handler.connect_service('slack', team_id, user_id,
                                   access_token=auth['access_token'],
                                   client=client,
                                   team_properties=team_properties,
                                   token_type='bearer')
    return handler, auth


@app.route('/slack/action', methods=['POST'])
@flask_extras.json_service()
def post_action():
    data = json.loads(request.form['payload'])
    if data['token'] != 'lwitiqXtcufDL1vXiMj3f36w':
        raise errors.InvalidArgument('Invalid data')
    if data['callback_id'] != 'review_content':
        raise errors.InvalidArgument('Unsupported callback_id')
    try:
        attachment = data['original_message']['attachments'][0]
    except:
        logging.exception('Missing attachment')
        raise errors.InvalidArgument('Missing attachment')
    try:
        action = data['actions'][0]
        assert action['name'] == 'quality'
        assert action['type'] == 'button'
        pieces = action['value'].split(':')
        account_id, content_id, quality = pieces
        account_id = int(account_id)
        content_id = int(content_id)
        if quality == 'hide':
            return {'text': ''}
        quality = int(quality)
    except:
        logging.exception('Error in action logic')
        raise errors.InvalidArgument('Unsupported action')
    if quality == 0:
        quality_label = u'1️⃣'
    elif quality == 1:
        quality_label = u'2️⃣'
    elif quality == 2:
        quality_label = u'3️⃣'
    elif quality == 3:
        quality_label = u'4️⃣'
    elif quality == 4:
        quality_label = u'🤩'
    else:
        quality_label = u'❓'
    title = attachment['title']
    username = data['user']['name']
    account_future = models.Account.get_by_id_async(account_id)
    taskqueue.add(url='/_ah/jobs/set_quality',
                  params={'account_id': account_id, 'quality': quality},
                  queue_name=config.INTERNAL_QUEUE)
    account = account_future.get_result()
    return {
        'text': u'%s rated %s: %s' % (username, slack_api.admin(account), quality_label),
    }


@app.route('/slack/auth', methods=['GET'])
def get_auth():
    slack_code = request.args.get('code')
    if not slack_code:
        logging.error('Missing "code" query string parameter')
        return 'Invalid request.', 400
    try:
        state = request.args.get('state')
        if state:
            data = json.loads(security.decrypt(config.SLACK_ENCRYPTION_KEY, state))
            if data['account_id']:
                account = models.Account.get_by_id(int(data['account_id']))
            else:
                account = None
        else:
            account = None
        handler, auth = _exchange_code('reactioncam', slack_code, account=account)
    except:
        logging.exception('Error exchanging Slack code')
        return 'Invalid request.', 400
    # Build the Roger authentication code for the client.
    session = handler.create_session()
    auth_code = session.create_auth_code('reactioncam')
    # Redirect to an app URI which will complete the login flow.
    redirect_uri = 'reactioncam://login/code/%s' % (auth_code.key.id(),)
    resp = make_response(RESPONSE_TEMPLATE % {'uri': redirect_uri})
    resp.headers['Location'] = redirect_uri
    return resp


@app.route('/slack/exchange', methods=['GET'])
@flask_extras.json_service()
def get_exchange():
    # Only reaction.cam can use this endpoint.
    try:
        payload = security.decrypt(config.WEB_ENCRYPTION_KEY,
                                   request.args['payload'],
                                   block_segments=True)
        handler, auth = _exchange_code('reactioncamweb', **json.loads(payload))
    except:
        logging.exception('Error exchanging Slack code')
        return 'Invalid request.', 400
    g.public_options['view_account'] = handler.account
    return handler.create_session(skip_activation=True)


@app.route('/slack/login', methods=['GET'])
def get_login():
    # If the user is already logged into an account, pass on its id.
    session = auth.get_session()
    state = convert.to_json({'account_id': session.account_id if session else None})
    scopes = [
        'channels:read',
        'chat:write:user',
        'groups:read',
        'users:read',
    ]
    qs = urllib.urlencode({
        'client_id': config.SLACK_CLIENT_ID,
        'scope': ' '.join(scopes),
        'state': security.encrypt(config.SLACK_ENCRYPTION_KEY, state),
    })
    redirect_uri = 'https://slack.com/oauth/authorize?%s' % (qs,)
    resp = make_response(RESPONSE_TEMPLATE % {'uri': redirect_uri})
    resp.headers['Location'] = redirect_uri
    return resp
