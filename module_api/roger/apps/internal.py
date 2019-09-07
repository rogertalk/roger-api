# -*- coding: utf-8 -*-

import json
import logging

from google.appengine.api import memcache
from google.appengine.ext import ndb

from flask import Flask, request

from roger import apps, config, models, notifs, streams
from roger_common import errors, flask_extras, security


app = Flask(__name__)
apps.set_up(app)


@app.route('/internal/chunk_played', methods=['POST'])
@flask_extras.json_service()
def post_chunk_played():
    # Only internal websites can use this endpoint.
    try:
        payload = security.decrypt(config.WEB_ENCRYPTION_KEY,
                                   flask_extras.get_parameter('payload'),
                                   block_segments=True)
        data = json.loads(payload)
        fingerprint = data['fingerprint']
        stream_key = ndb.Key('Stream', data['stream_id'])
        chunk_key = ndb.Key('Chunk', data['chunk_id'], parent=stream_key)
    except:
        raise errors.InvalidArgument('Invalid payload')
    cache_key = 'external_plays:%d:%d:%s' % (stream_key.id(), chunk_key.id(), fingerprint)
    if memcache.get(cache_key):
        logging.debug('Repeat chunk play for fingerprint %s (stream %d chunk %d)',
                      fingerprint, stream_key.id(), chunk_key.id())
        return {'success': True}
    memcache.set(cache_key, True, 172800)
    stream, chunk = ndb.get_multi([stream_key, chunk_key])
    if not stream or not chunk:
        raise errors.ResourceNotFound('That chunk does not exist')
    chunk.external_plays += 1
    chunk.put()
    for local_chunk in stream.chunks:
        if local_chunk.chunk_id == chunk.key.id():
            local_chunk.external_plays = chunk.external_plays
            stream.put()
            break
    logging.debug('New chunk play for fingerprint %s (stream %d chunk %d)',
                  fingerprint, stream_key.id(), chunk_key.id())
    logging.debug('Total external chunk plays is now %d', chunk.external_plays)
    handler = streams.MutableStream(chunk.sender, stream)
    if chunk.external_plays == 1:
        handler.notify_first_play(chunk)
    handler.notify(notifs.ON_STREAM_CHUNK_EXTERNAL_PLAY, add_stream=True, chunk=chunk)
    return {'success': True}


@app.route('/internal/transcode_complete', methods=['POST'])
@flask_extras.json_service()
def post_transcode_complete():
    try:
        content_id = int(flask_extras.get_parameter('content_id'))
    except:
        raise errors.InvalidArgument('Invalid content_id parameter')
    stream_url = flask_extras.get_parameter('stream_url')
    if not stream_url:
        raise errors.InvalidArgument('Invalid stream_url parameter')
    content = models.Content.get_by_id(content_id)
    if not content:
        raise errors.ResourceNotFound('Content not found')
    if not content.metadata:
        content.metadata = {}
    content.metadata['raw_video_url'] = content.video_url
    content.video_url = stream_url
    content.put()
    return {'success': True}
