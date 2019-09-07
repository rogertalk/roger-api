# -*- coding: utf-8 -*-

import logging

from google.appengine.api import taskqueue

from flask import Flask, g, request

from roger import accounts, apps, auth, config, report, services
from roger.apps import utils
from roger_common import errors, events, flask_extras


app = Flask(__name__)
apps.set_up(app)


def grant_type_authorization_code(client):
    code = flask_extras.get_parameter('code')
    if not code:
        raise errors.MissingArgument('A code is required')
    redirect_uri = flask_extras.get_parameter('redirect_uri')
    if not redirect_uri:
        redirect_uri = None
    if redirect_uri and redirect_uri not in client.redirect_uris:
        raise errors.InvalidArgument('Invalid redirect_uri value')
    session = auth.Session.from_auth_code(code, client.key.id(), redirect_uri)
    return accounts.get_handler(session.account)


def grant_type_password(client):
    username = flask_extras.get_parameter('username')
    password = flask_extras.get_parameter('password')
    if not username:
        raise errors.MissingArgument('A username is required')
    if not password:
        raise errors.MissingArgument('A password is required')

    try:
        user = accounts.get_handler(username)
    except errors.ResourceNotFound:
        # Return a 401 instead of a 404 when the account does not exist.
        raise errors.InvalidCredentials()

    if not user.validate_password(password):
        raise errors.InvalidCredentials()

    report.user_logged_in(user.account_id, auth_identifier=username,
                          challenge='password')
    return user


def grant_type_refresh_token(client):
    refresh_token = flask_extras.get_parameter('refresh_token')
    if not refresh_token:
        raise errors.MissingArgument('A refresh token is required')
    try:
        session = auth.Session.from_refresh_token(refresh_token)
    except ValueError:
        logging.exception('Failed to restore refresh token')
        raise errors.InvalidCredentials()
    return accounts.get_handler(session.account)


@app.route('/oauth2/token', methods=['POST'])
@flask_extras.json_service()
def request_token():
    """
    Attempts to log the user in with the specified grant type.

    Request:
        POST /oauth2/token?grant_type=password&client_id=ios
        username=bob&password=1234

    Response:
        {
            "access_token": "RLDvsbckw7tJJCiCPzU9bF",
            "refresh_token": "pArhTbEs8ex1f79vAqxR2",
            "token_type": "bearer",
            "expires_in": 3600,
            "account": {
                "id": 12345678,
                "username": "bob",
                "status": "active"
            }
        }

    """
    client_id, client_secret = auth.get_client_details()
    if not client_id:
        raise errors.MissingArgument('A client id is required')
    client = services.get_client(client_id)
    if client_secret != client.client_secret:
        # TODO: Enforce a valid client_secret.
        logging.warning('Client secret appears invalid (client_id=%s)', client_id)
    grant_type = flask_extras.get_parameter('grant_type')
    if grant_type == 'authorization_code':
        user = grant_type_authorization_code(client)
    elif grant_type == 'password':
        user = grant_type_password(client)
    elif grant_type == 'refresh_token':
        user = grant_type_refresh_token(client)
    else:
        raise errors.NotSupported('Unsupported grant type "{}"'.format(grant_type))
    # Report the successful token exchange.
    event = events.TokenExchangeV1(user.account.key.id(),
                                   client_id=client_id,
                                   grant_type=grant_type)
    event.report()
    # Track all fika.io logins.
    if grant_type != 'refresh_token':
        taskqueue.add(url='/_ah/jobs/track_login',
                      countdown=20,
                      params={'account_id': user.account_id, 'client_id': client_id},
                      queue_name=config.INTERNAL_QUEUE)
    # Return the session and some additional data about the user.
    data = utils.get_streams_data(user.account)
    session = user.create_session(extra_data=data)
    g.public_options['include_extras'] = True
    g.public_options['view_account'] = session.account
    return session
