# -*- coding: utf-8 -*-

import base64
import json
import logging
import re
import urllib

from google.appengine.ext import ndb

from roger import config, models
from roger_common import errors


# NOTE: Access token belongs to account 5140669573103616 (@placeholder).
ACCESS_TOKEN = 'zCuB4oc_P7bKwqvQTdYEP9rQXdIGkHF0LBC87hfuuT8oIB8'


@ndb.tasklet
def auth_async(code):
    try:
        result = yield _fetch_async(
            'https://www.googleapis.com/oauth2/v4/token',
            method='POST',
            payload=urllib.urlencode({
                'client_id': '_REMOVED_',
                'code': code,
                'grant_type': 'authorization_code',
                'redirect_uri': 'cam.reaction.ReactionCam:/youtube_oauth2redirect',
            }),
            follow_redirects=False,
            deadline=10)
        data = json.loads(result.content)
    except Exception as e:
        logging.exception('YouTube token exchange failed.')
        raise errors.ServerError()
    if result.status_code != 200:
        logging.debug('Could not exchange code: %r', data)
        raise errors.InvalidArgument('Failed to exchange code for token')
    try:
        id_token = data.pop('id_token')
        id_hdr, id_bdy, id_sig = (p + ('=' * (-len(p) % 4)) for p in id_token.split('.'))
        profile = json.loads(base64.b64decode(id_bdy))
        assert 'sub' in profile
    except:
        logging.debug('Could not extract profile data: %r', data)
        raise errors.InvalidArgument('Missing profile in token response')
    raise ndb.Return((data, profile))


def generate_description(creator, content, related_to):
    description = u'REQUEST MY NEXT VIDEO: http://rcam.at/%s' % (creator.username,)
    # Add credit link for reactions.
    if related_to:
        if description: description += u'\n\n'
        if related_to.original_url:
            description += u'Reaction to %s (%s)' % (related_to.title or u'Original', related_to.original_url)
        elif related_to.title:
            description += u'Reaction to %s' % (related_to.title,)
    # Add link directly to video on reaction.cam.
    if content.web_url:
        if description: description += u'\n\n'
        description += u'Comment and chat: %s' % (content.web_url,)
    # Assemble a list of social networks to promote in description.
    # TODO: Strip out URL format.
    networks = []
    # Facebook
    network = creator.properties.get('facebook', None)
    if network:
        networks.append((u'Facebook', network))
    # Instagram
    network = creator.properties.get('instagram', None)
    if network:
        networks.append((u'Instagram', network))
    # Musical.ly
    network = creator.properties.get('musically', None)
    if network:
        networks.append((u'Musical.ly', network))
    # Snapchat
    network = creator.properties.get('snapchat', None)
    if network:
        networks.append((u'Snapchat', network))
    # Twitter
    network = creator.properties.get('twitter', None)
    if network:
        networks.append((u'Twitter', network))
    # Add the list of social networks to description.
    if networks:
        if description: description += u'\n\n'
        description += u'Follow me on:\n'
        description += u'\n'.join(u'%s: %s' % (n, v) for n, v in networks)
    return description


def generate_title(creator, content, related_to):
    suffix = u' – REACTION.CAM'
    if content.title:
        cut_off = 90
        cut_off -= len(suffix)
        t = content.title.strip()
        if t.endswith(u'REACTION.CAM'):
            t = t[:-12]
        t = t.rstrip(u'  -–')
        if len(t) > cut_off:
            t = t[:cut_off-1].rstrip() + u'…'
        title = t
    elif related_to and related_to.title:
        cut_off = 90
        cut_off -= len(suffix)
        cut_off -= 9
        t = related_to.title.strip()
        if t.endswith(u'REACTION.CAM'):
            t = t[:-12]
        t = t.rstrip(u'  -–')
        if len(t) > cut_off:
            t = t[:cut_off-1].rstrip() + u'…'
        title = u'%s REACTION' % (t,)
    else:
        title = u'VLOG UPDATE by @%s' % (creator.username,)
    if u'REACTION.CAM' not in title:
        title += suffix
    return title


@ndb.tasklet
def get_channels_async(youtube_refresh_token):
    try:
        qs = urllib.urlencode({
            'youtube_token': youtube_refresh_token,
        })
        result = yield _fetch_async(
            'https://upload.reaction.cam/v2/channels?%s' % (qs,),
            follow_redirects=False,
            headers={'Authorization': 'Bearer ' + ACCESS_TOKEN},
            deadline=10)
        data = json.loads(result.content)
    except Exception as e:
        logging.exception('YouTube call failed.')
        raise errors.ServerError()
    if result.status_code in (400, 401):
        # TODO: We should probably invalidate the token.
        raise ndb.Return([])
    if result.status_code != 200:
        logging.debug('Could not get channels (%d): %r', result.status_code, data)
        raise errors.ExternalError('Could not get channels')
    raise ndb.Return(data['channels'])


@ndb.tasklet
def get_video_async(video_id, snippet=True, statistics=False):
    if not (snippet or statistics):
        raise errors.InvalidArgument('Specify at least one of snippet, statistics')
    parts = []
    if snippet:
        parts.append('snippet')
    if statistics:
        parts.append('statistics')
    try:
        qs = urllib.urlencode({
            'id': video_id,
            'key': config.YOUTUBE_API_KEY,
            'part': ','.join(parts),
        })
        result = yield _fetch_async(
            'https://www.googleapis.com/youtube/v3/videos?%s' % (qs,),
            follow_redirects=False,
            deadline=10)
        data = json.loads(result.content)
    except Exception as e:
        logging.exception('YouTube call failed.')
        raise errors.ServerError()
    if result.status_code == 404:
        raise errors.ResourceNotFound('That video does not exist')
    elif result.status_code != 200:
        logging.debug('Could not get YouTube video (%d): %r', result.status_code, data)
        raise errors.InvalidArgument('Invalid video id')
    if data['pageInfo']['totalResults'] == 0:
        raise ndb.Return(None)
    if not data['items']:
        logging.warning('Expected at least one item in data: %r', data)
        raise ndb.Return(None)
    raise ndb.Return(data['items'][0])


@ndb.tasklet
def get_videos_async(youtube_refresh_token, limit=None):
    try:
        qs = {'youtube_token': youtube_refresh_token}
        if limit is not None:
            qs['limit'] = str(limit)
        result = yield _fetch_async(
            'https://upload.reaction.cam/v2/videos?%s' % (urllib.urlencode(qs),),
            follow_redirects=False,
            headers={'Authorization': 'Bearer ' + ACCESS_TOKEN},
            deadline=10)
        data = json.loads(result.content)
    except Exception as e:
        logging.exception('YouTube call failed.')
        raise errors.ServerError()
    if result.status_code in (400, 401):
        # TODO: We should probably invalidate the token.
        raise ndb.Return([])
    if result.status_code != 200:
        logging.debug('Could not get videos (%d): %r', result.status_code, data)
        raise errors.ExternalError('Could not get videos')
    raise ndb.Return(data['videos'])


@ndb.tasklet
def upload_async(creator, content, related_to=None, token=None):
    assert isinstance(creator, models.Account)
    assert isinstance(content, models.Content)
    assert related_to is None or isinstance(related_to, models.Content)
    assert isinstance(token, basestring)
    if not content.is_public:
        logging.warning('Not uploading content to YouTube because it is not public')
        return
    if content.related_to:
        if not related_to or related_to.key != content.related_to:
            related_to = yield content.related_to.get_async()
    else:
        related_to = None
    title = generate_title(creator, content, related_to)
    description = generate_description(creator, content, related_to)
    # Create a set of tags to add to the YouTube video.
    tags = []
    tags_set = set()
    def add_tag(tag):
        if not isinstance(tag, basestring):
            return
        tag = tag.replace(u'"', u'').replace(u',', u'')
        tag = re.sub(r'^\s+', u' ', tag).strip()
        tag_normalized = tag.lower()
        if not tag_normalized or tag_normalized in tags_set:
            return
        tags_set.add(tag_normalized)
        if u' ' in tag:
            tag = u'"%s"' % (tag,)
        tags.append(tag)
    if related_to:
        if related_to.title:
            add_tag(related_to.title + u' REACTION')
            add_tag(u'react to ' + related_to.title)
        if related_to.properties:
            add_tag(related_to.properties.get('title_short'))
            add_tag(related_to.properties.get('creator_label'))
    for tag in content.visible_tags:
        if ' ' in tag or tag in ('featured', 'exfeatured', 'original'):
            continue
        add_tag(tag)
    add_tag('lyrics')
    add_tag('remix')
    add_tag('reaccion')
    add_tag('reaction.cam')
    tags_string = u''
    for tag in tags:
        piece = u',' + tag if tags_string else tag
        if len(tags_string) + len(piece) > 500:
            continue
        tags_string += piece
    # Build the payload for YouTube.
    form = {
        'content_id': str(content.key.id()),
        'creator_id': str(creator.key.id()),
        'description': description.encode('utf-8'),
        'tags': tags_string.encode('utf-8'),
        'title': title.encode('utf-8'),
        'url': content.video_url,
        'youtube_token': token,
    }
    if config.DEVELOPMENT:
        form['dry_run'] = 'true'
    if related_to:
        logging.debug('Starting YouTube upload of reaction to %d: %d by %d',
            related_to.key.id(),
            content.key.id(),
            creator.key.id())
    else:
        logging.debug('Starting YouTube upload of %d by %d (not a reaction)',
            content.key.id(),
            creator.key.id())
    result = yield _fetch_async(
        'https://upload.reaction.cam/v2/youtube',
        payload=urllib.urlencode(form),
        method='POST',
        headers={
            'Authorization': 'Bearer ' + ACCESS_TOKEN,
            'Content-Type': 'application/x-www-form-urlencoded'
        },
        follow_redirects=False,
        deadline=10)
    if result.status_code == 200:
        logging.debug('Successfully queued YouTube upload')
    elif result.status_code == 409:
        logging.warning('Upload service refused request (already uploading)')
        raise errors.AlreadyInProgress()
    else:
        logging.error('Upload service returned status %s', result.status_code)


def _fetch_async(*args, **kwargs):
    # Separate function to make it easier to mock.
    context = ndb.get_context()
    return context.urlfetch(*args, **kwargs)
