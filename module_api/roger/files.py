# -*- coding: utf-8 -*-

import base64
from datetime import datetime
import hashlib
import hmac
import json
import logging
import mimetypes
import os
import os.path
import re
import shutil
import sys
import urllib
import warnings

from google.appengine.api import app_identity, urlfetch

import cloudstorage as gcs
from urllib3 import PoolManager
from urllib3.contrib.appengine import AppEngineManager, is_appengine_sandbox

from roger import config
from roger_common import errors


if is_appengine_sandbox():
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        http = AppEngineManager()
else:
    http = PoolManager()


# Load the AWS secrets from disk.
with open(config.AWS_SECRETS_FILE) as fh:
    __aws_secrets = json.load(fh)


def download(path, destination_file_object):
    # Load the file and output it.
    with gcs.open(_absolute(path), 'r') as f:
        shutil.copyfileobj(f, destination_file_object)


def is_persistent(path):
    return _absolute(path).startswith(config.STORAGE_PATH_PERSISTENT + '/')


def is_shortlived(path):
    return _absolute(path).startswith(config.STORAGE_PATH_SHORTLIVED + '/')


def legacy_url(path):
    if not path:
        return None
    if path.startswith('http:') or path.startswith('https:'):
        # It's already a URL.
        return path
    path = _absolute(path)
    directory, basename, extension = _path_parts(path)
    if is_persistent(path):
        basename += '-p'
    elif not is_shortlived(path):
        raise ValueError('Invalid path %r' % (path,))
    return config.FILE_URL_FORMAT % (basename + extension,)


def make_persistent(path):
    """Marks a file as persistent, which means it won't be deleted automatically."""
    path = _absolute(path)
    if not is_shortlived(path):
        return
    new_path = config.STORAGE_PATH_PERSISTENT + path[len(config.STORAGE_PATH_SHORTLIVED):]
    gcs.copy2(path, new_path)
    return new_path


def storage_url(path_or_url):
    if not path_or_url:
        return None
    if path_or_url.startswith('http:') or path_or_url.startswith('https:'):
        url = path_or_url
    else:
        path = _absolute(path_or_url)
        bucket, _, path = path.lstrip('/').partition('/')
        url = config.STORAGE_URL_FORMAT % (bucket, path)
    for origin in config.STORAGE_LEGACY_ORIGINS:
        if not url.startswith(origin):
            continue
        url = url.replace(origin, config.STORAGE_LEGACY_CDN)
        break
    return url


def upload(original_filename, stream, persist=False):
    # Read the entire stream.
    stream.seek(0)
    data = stream.read()
    return _upload(original_filename, data, persist)


def upload_from_url(url, persist=False):
    result = urlfetch.fetch(url)
    if result.status_code != 200:
        raise errors.ExternalError('Failed to download file')
    return _upload(url, result.content, persist)


def _absolute(path):
    if path.startswith('/'):
        return path
    match = re.match('^https?://[^/]+/(?:audio|file|image)/(.+)', path)
    if match:
        # Convert full URLs into Cloud Storage paths.
        # TODO: Remove this once we only use Cloud Storage paths.
        path = match.group(1)
    directory, basename, extension = _path_parts(path)
    if basename.endswith('-p'):
        gcs_root = config.STORAGE_PATH_PERSISTENT
        basename = basename[:-2]
    elif extension in ('.jpg', '.png'):
        # Images should generally be persistent.
        # TODO: Remove this once we only use Cloud Storage paths.
        gcs_root = config.STORAGE_PATH_PERSISTENT
    else:
        gcs_root = config.STORAGE_PATH_SHORTLIVED
    return os.path.join(gcs_root, directory, basename + extension)


def _aws4_signed_fetch(region, service, headers, method, host, path, qs=None, payload=None, payload_sha256=None):
    if not qs:
        qs = ''
    if not payload:
        payload = ''
    if not payload_sha256:
        payload_sha256 = hashlib.sha256(payload).hexdigest()
    # Prepare timestamps to use.
    now = datetime.utcnow()
    datestamp = now.strftime('%Y%m%d')
    timestamp = now.strftime('%Y%m%dT%H%M%SZ')
    # Set a few common AWS headers.
    headers = dict(headers)
    headers['Host'] = host
    headers['x-amz-date'] = timestamp
    # Assemble information about the HTTP request.
    header_keys_to_sign = {'host', 'x-amz-acl', 'x-amz-content-sha256', 'x-amz-date'}
    signed_header = [(k, v) for k, v in headers.iteritems()
                     if k.lower() in header_keys_to_sign]
    signed_header.sort(key=lambda (k, v): k.lower())
    signed_header_keys = ';'.join(k.lower() for k, _ in signed_header)
    headers_string = '\n'.join(k.lower() + ':' + v for k, v in signed_header) + '\n'
    request_string = '\n'.join([method, path, qs, headers_string, signed_header_keys, payload_sha256])
    # Assemble the string to be signed.
    scope = '/'.join([datestamp, region, service, 'aws4_request'])
    algo = 'AWS4-HMAC-SHA256'
    string_to_sign = '\n'.join([algo, timestamp, scope, hashlib.sha256(request_string).hexdigest()])
    # Get the secret keys needed to sign the request.
    try:
        access_key = __aws_secrets['access_key_id'].encode('utf-8')
        secret_key = __aws_secrets['secret_access_key'].encode('utf-8')
    except:
        access_key = ''
        secret_key = ''
    # Create the signature with which to verify that the request is valid.
    sig = hmac.new('AWS4' + secret_key, datestamp, hashlib.sha256).digest()
    sig = hmac.new(sig, region, hashlib.sha256).digest()
    sig = hmac.new(sig, service, hashlib.sha256).digest()
    sig = hmac.new(sig, 'aws4_request', hashlib.sha256).digest()
    sig = hmac.new(sig, string_to_sign, hashlib.sha256).hexdigest()
    # Assemble remaining parts for HTTP headers.
    auth_parts = [
        'Credential=%s/%s' % (access_key, scope),
        'SignedHeaders=' + signed_header_keys,
        'Signature=' + sig,
    ]
    headers['Authorization'] = '%s %s' % (algo, ', '.join(auth_parts))
    url = 'https://%s%s' % (host, path)
    if qs:
        url += '?' + qs
    return urlfetch.fetch(method=method, url=url, headers=headers, payload=payload)


def _media_service(service, fields={}, persist=False, **kwargs):
    kwargs['bucket'] = config.BUCKET_PERSISTENT if persist else config.BUCKET_SHORTLIVED
    url = '{}?{}'.format(service, urllib.urlencode(kwargs))
    logging.debug('POST %s', url)
    result = http.request('POST', url, fields=fields, timeout=60)
    if result.status != 200:
        logging.warning('HTTP %d: %s', result.status, result.data)
        if result.status == 404:
            raise errors.InvalidArgument('The provided URL could not be loaded')
        raise errors.ExternalError('Failed to process media')
    info = json.loads(result.data.decode('utf-8'))
    path = '/' + info['bucket_path']
    return path, info['duration']


def _clean_url_cb(match):
    no_scheme = match.group(1)
    if len(no_scheme) > 25:
        return ''
    no_scheme = no_scheme.replace('-', '-dash-')
    no_scheme = no_scheme.replace('www.', 'dub-dub-dub.')
    no_scheme = no_scheme.replace('/', '-slash-')
    return no_scheme


def _clean_urls(text):
    return re.sub(r'\bhttps?://([^?#\s]*)\S*', _clean_url_cb, text)


def _path_parts(path):
    directory, _, filename = path.rpartition('/')
    basename, dot, extension = filename.partition('.')
    return directory, basename, dot + extension


def _gcs_upload_shortlived(data, data_sha256, extension, mime_type):
    path = os.path.join(config.STORAGE_PATH_SHORTLIVED, data_sha256 + extension)
    options = {'Cache-Control': 'public, max-age=172800'}
    with gcs.open(path, mode='w', content_type=mime_type, options=options) as f:
        f.write(data)
    return path


def _s3_upload(data, data_sha256, extension, mime_type):
    filename = data_sha256 + extension
    try:
        result = _aws4_signed_fetch(region='us-east-1', service='s3',
                                    headers={'x-amz-acl': 'public-read',
                                             'x-amz-content-sha256': data_sha256,
                                             'Content-Type': mime_type},
                                    method='PUT', host='s3.amazonaws.com',
                                    path='/%s/%s' % (config.S3_BUCKET, filename),
                                    payload=data, payload_sha256=data_sha256)
    except:
        logging.exception('Could not upload to S3.')
        raise errors.ServerError()
    if result.status_code != 200:
        logging.debug('Could not upload file: %r', result.content)
        raise errors.InvalidArgument('Failed to upload file')
    return config.S3_BUCKET_CDN + filename


def _upload(original_filename, data, persist):
    if not data:
        raise errors.InvalidArgument('Empty data')
    if persist:
        upload_func = _s3_upload
    else:
        upload_func = _gcs_upload_shortlived
    # Create a unique filename from the data and file extension.
    _, extension = os.path.splitext(original_filename)
    data_sha256 = hashlib.sha256(data).hexdigest()
    mime_type, _ = mimetypes.guess_type(original_filename)
    return upload_func(data, data_sha256, extension.lower(), mime_type)
