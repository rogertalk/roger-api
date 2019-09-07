# -*- coding: utf-8 -*-

import logging

from google.appengine.ext import ndb

from roger import models
from roger_common import errors


def get_client(client_id):
    client = models.ServiceClient.get_by_id(client_id)
    if not client:
        raise errors.InvalidArgument('Invalid client')
    return Client(client)


def get_connected_and_featured(account, category=None):
    account = models.Account.resolve(account)
    if not account:
        raise ValueError('Could not resolve provided account')
    q = models.Service.query()
    q = q.filter(models.Service.featured > 0)
    if category == 'service' and account.is_employee:
        category = 'service_employee'
    if category:
        q = q.filter(models.Service.categories == category)
    q = q.order(models.Service.featured)
    featured_future = q.fetch_async()
    connected = models.ServiceAuth.query(ancestor=account.key)
    featured = featured_future.get_result()
    # Add all the connected services.
    services = []
    missing_keys = []
    for auth in connected:
        for i, service in enumerate(featured):
            if service.key == auth.service:
                break
        else:
            missing_keys.append(auth.service)
            continue
        featured.pop(i)
        services.append(Service(account, service, auth))
    if missing_keys:
        # One or more connected services were not featured.
        missing = ndb.get_multi(missing_keys)
        for service in missing:
            for auth in connected:
                if auth.service == service.key:
                    break
            else:
                logging.error('Could not find correct ServiceAuth')
                continue
            if category and category not in service.categories:
                continue
            services.append(Service(account, service, auth))
    # Add the non-connected, featured services.
    for service in featured:
        services.append(Service(account, service, None))
    return services


class Client(object):
    # Avoid accidentally setting unsupported attributes.
    __slots__ = ['_client']

    def __getattr__(self, name):
        # By default, proxy to the underlying ServiceClient entity.
        return getattr(self._client, name)

    def __init__(self, client):
        if not isinstance(client, models.ServiceClient):
            raise TypeError('Expected a client')
        self._client = client

    def public(self, version=None, **kwargs):
        return {
            'id': self._client.key.id(),
            'description': self._client.description,
            'image_url': self._client.image_url,
            'title': self._client.title,
        }


class Service(object):
    # Avoid accidentally setting unsupported attributes.
    __slots__ = ['account', '_auth', '_service']

    def __getattr__(self, name):
        # By default, proxy to the underlying Service entity.
        return getattr(self._service, name)

    def __init__(self, me, service, auth):
        self.account = models.Account.resolve(me)
        if not self.account:
            raise ValueError('Expected an account')
        if not isinstance(service, models.Service):
            raise TypeError('Expected a service')
        self._service = service
        if auth is not None and not isinstance(auth, models.ServiceAuth):
            raise TypeError('Expected None or auth data')
        self._auth = auth

    def public(self, version=None, **kwargs):
        bot_key = self._service.account
        result = {
            'account_id': bot_key.id() if bot_key else None,
            'id': self._service.key.id(),
            'connect_url': self._service.connect_url,
            'connected': self._service.connect_url is None or self._auth is not None,
            'description': self._service.description,
            'finish_pattern': self._service.finish_pattern,
            'image_url': self._service.image_url,
            'title': self._service.title,
        }
        if version >= 21:
            result['client_code'] = self._service.client_code
        return result
