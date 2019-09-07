# -*- coding: utf-8 -*-

from datetime import datetime

from google.appengine.api import memcache

from roger import config


def spend(*args, **kwargs):
    num_tokens = kwargs.get('num_tokens', 1)
    config_key = ':'.join(args) if args else 'default'
    cache_key = 'ratelimit:{}'.format(config_key)
    if config_key not in config.RATELIMITS:
        if not args:
            return True
        # Allow the last argument to be a wildcard.
        # TODO: More elaborate matching rules.
        config_key = ':'.join(args[:-1] + ('*',))
        if config_key not in config.RATELIMITS:
            return True
    size, rate = config.RATELIMITS[config_key]
    if num_tokens > size:
        return False
    now = datetime.utcnow()
    tokens, timestamp = memcache.get(cache_key) or (size, now)
    tokens = min(size, tokens + (now - timestamp).total_seconds() * rate)
    if tokens - num_tokens < 0:
        return False
    tokens -= num_tokens
    memcache.set(cache_key, (tokens, now), (size - tokens) / rate)
    return True
