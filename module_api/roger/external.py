# -*- coding: utf-8 -*-

import base64
import json
import logging
import random
import re
import urllib

from google.appengine.api import urlfetch

from roger import config, localize, slack_api
from roger_common import errors, identifiers


def _phone_call_messagebird_challenge(number, args):
    url = 'https://rest.messagebird.com/voicemessages'
    code = '<break time=".5s"/>'.join(list(str(args['code'])))
    body = localize.get_string('call.challenge_code', args={'code': code}, receiver=number)
    params = {
        'recipients': number[1:],
        'ifMachine': 'continue',
        'voice': 'female',
        'body': body.encode('utf-8') if isinstance(body, unicode) else body,
    }
    headers = {
        'Authorization': 'AccessKey %s' % config.SMS_MESSAGEBIRD_API_TOKEN,
    }
    try:
        result = urlfetch.fetch(
            url=url,
            method=urlfetch.POST,
            payload=urllib.urlencode(params),
            headers=headers,
            )
        data = json.loads(result.content)
    except Exception as e:
        logging.exception('MessageBird API request failed.')
        return -1, {'code': -1, 'message': str(e)}
    if not 200 <= result.status_code < 300:
        logging.warning('MessageBird API error %d: %s (%s)', result.status_code,
                        data.get('message'), data.get('code'))
    return result.status_code, data


def _phone_call_nexmo_challenge(number, args):
    url_format = 'https://api.nexmo.com/tts/json?%s'
    code = '<break time=".5s"/>'.join(list(str(args['code'])))
    body = localize.get_string('call.challenge_code', args={'code': code}, receiver=number)
    params = {
        'api_key': config.SMS_NEXMO_API_ACCOUNT,
        'api_secret': config.SMS_NEXMO_API_TOKEN,
        'to': number[1:],
        'text': body.encode('utf-8') if isinstance(body, unicode) else body,
    }
    if number.startswith('+1'):
        params['from'] = config.CALL_NEXMO_FROM_NUMBER
    try:
        result = urlfetch.fetch(url=url_format % (urllib.urlencode(params)))
        data = json.loads(result.content)
    except Exception as e:
        logging.exception('Nexmo API request failed.')
        return -1, {'code': -1, 'message': str(e)}
    if not 200 <= result.status_code < 300:
        logging.warning('Nexmo API error %d: %s (%s)', result.status_code,
                        data.get('message'), data.get('code'))
    return result.status_code, data


def _send_sms_messagebird(number, body):
    url = 'https://rest.messagebird.com/messages'
    params = {
        'recipients': number[1:],
        'originator': 'Roger',
        'validity': 86400,
        'body': body.encode('utf-8') if isinstance(body, unicode) else body,
    }
    headers = {
        'Authorization': 'AccessKey %s' % config.SMS_MESSAGEBIRD_API_TOKEN,
    }
    try:
        result = urlfetch.fetch(
            url=url,
            method=urlfetch.POST,
            payload=urllib.urlencode(params),
            headers=headers,
            )
        data = json.loads(result.content)
    except Exception as e:
        logging.exception('MessageBird API request failed.')
        return -1, {'code': -1, 'message': str(e)}
    if not 200 <= result.status_code < 300:
        logging.warning('MessageBird API error %d: %s (%s)', result.status_code,
                        data.get('message'), data.get('code'))
    return result.status_code, data


def _send_sms_nexmo(number, body):
    url_format = 'https://rest.nexmo.com/sms/json?%s'
    params = {
        'api_key': config.SMS_NEXMO_API_ACCOUNT,
        'api_secret': config.SMS_NEXMO_API_TOKEN,
        'from': 'Roger',
        'to': number[1:],
        'text': body.encode('utf-8') if isinstance(body, unicode) else body,
        'type': 'unicode',
    }
    try:
        result = urlfetch.fetch(url=url_format % (urllib.urlencode(params)))
        data = json.loads(result.content)
    except Exception as e:
        logging.exception('Nexmo API request failed.')
        return -1, {'code': -1, 'message': str(e)}
    if not 200 <= result.status_code < 300:
        logging.warning('Nexmo API error %d: %s (%s)', result.status_code,
                        data.get('message'), data.get('code'))
    return result.status_code, data


def _send_sms_nexmo_challenge(number, args):
    url_format = 'https://rest.nexmo.com/sc/us/2fa/json?%s'
    params = {
        'api_key': config.SMS_NEXMO_API_ACCOUNT,
        'api_secret': config.SMS_NEXMO_API_TOKEN,
        'to': number[1:],
        'pin': args['code'],
    }
    try:
        result = urlfetch.fetch(url=url_format % (urllib.urlencode(params)))
        data = json.loads(result.content)
    except Exception as e:
        logging.exception('Nexmo API request failed.')
        return -1, {'code': -1, 'message': str(e)}
    if not 200 <= result.status_code < 300:
        logging.warning('Nexmo API error %d: %s (%s)', result.status_code,
                        data.get('message'), data.get('code'))
    return result.status_code, data


def _send_sms_nexmo_token(number, args):
    url_format = 'https://rest.nexmo.com/sc/us/alert/json?%s'
    params = {
        'api_key': config.SMS_NEXMO_API_ACCOUNT,
        'api_secret': config.SMS_NEXMO_API_TOKEN,
        'to': number[1:],
        'name': args['name'],
        'link': args['link'],
    }
    try:
        result = urlfetch.fetch(url=url_format % (urllib.urlencode(params)))
        data = json.loads(result.content)
    except Exception as e:
        logging.exception('Nexmo API request failed.')
        return -1, {'code': -1, 'message': str(e)}
    if not 200 <= result.status_code < 300:
        logging.warning('Nexmo API error %d: %s (%s)', result.status_code,
                        data.get('message'), data.get('code'))
    return result.status_code, data


def _send_sms_slack(number, body):
    attachment = slack_api.attachment(
        'Sending SMS to fictional number %s: %s' % (number, body),
        title='SMS to fictional number',
        fields=[
            slack_api.short_field('Number', number),
            slack_api.short_field('Text', body),
        ]
    )
    slack_api.message(attachments=[attachment])


def _send_sms_twilio(number, body, from_number=config.SMS_TWILIO_API_NUMBER):
    url_format = 'https://api.twilio.com/2010-04-01/Accounts/%s/Messages.json'
    payload = {
        'From': config.SMS_TWILIO_API_NUMBER,
        'To': number,
        'Body': body.encode('utf-8') if isinstance(body, unicode) else body,
        }
    return _twilio_request(url_format % config.SMS_TWILIO_API_ACCOUNT, payload)


def _twilio_request(url, payload=None):
    api_credentials = base64.b64encode('%s:%s' % (
        config.SMS_TWILIO_API_ACCOUNT,
        config.SMS_TWILIO_API_TOKEN,
        ))
    headers = {
        'Authorization': 'Basic %s' % api_credentials,
        'Content-Type': 'application/x-www-form-urlencoded',
        }
    try:
        result = urlfetch.fetch(
            url=url,
            method=urlfetch.POST if payload else urlfetch.GET,
            payload=urllib.urlencode(payload) if payload else None,
            headers=headers,
            )
        data = json.loads(result.content)
    except Exception as e:
        logging.debug('Failing payload: %r', payload)
        logging.exception('Twilio API request failed')
        return -1, {'code': -1, 'message': str(e)}
    if not 200 <= result.status_code < 300:
        logging.warning('Twilio API error %d: %s (%s)', result.status_code,
                        data.get('message'), data.get('code'))
    return result.status_code, data


def phone_call(number, string_id, args):
    body = localize.get_string('call.%s' % string_id, args=args, receiver=number)
    if config.DEVELOPMENT:
        logging.debug('Calling "%s" with %s', number, body)
    if re.match('^\\+1...55501..$', number):
        # Forward calls to movie numbers into the Slack channel.
        return _send_sms_slack(number, body)
    if False and random.randint(1, 10) == 1:
        logging.info('Phone call to %s using MessageBird API', number)
        status, data = _phone_call_messagebird_challenge(number, args)
    else:
        logging.info('Phone call to %s using Nexmo API', number)
        status, data = _phone_call_nexmo_challenge(number, args)
    if not 200 <= status < 300:
        raise errors.ExternalError('Failed to call phone')


def format_number(number):
    url_format = 'https://lookups.twilio.com/v1/PhoneNumbers/%s'
    status, data = _twilio_request(url_format % number)
    if not 200 <= status < 300:
        return number
    return data['national_format']


def send_sms(number, string_id, args):
    body = localize.get_string('sms.%s' % string_id, args=args, receiver=number)
    if config.DEVELOPMENT:
        logging.debug('Sending SMS "%s" to %s', body, number)
    if re.match('^\\+1...55501..$', number):
        # Forward messages to movie numbers into the Slack channel.
        _send_sms_slack(number, body)
        return
    if string_id in ('invite', 'invite_personal'):
        # Disable all invite SMS.
        return
    if False and random.randint(1, 10) == 1:
        logging.info('Sending SMS to %s using MessageBird API', number)
        status, data = _send_sms_messagebird(number, body)
    elif number.startswith('+1'):
        # Twilio until we get a shortcode template in Nexmo or Branch.io.
        logging.info('Sending SMS to %s using Twilio API', number)
        if string_id == 'download_link':
            status, data = _send_sms_twilio(number, body, from_number='+14427776437')
        else:
            status, data = _send_sms_twilio(number, body)
    else:
        logging.info('Sending SMS to %s using Nexmo API', number)
        status, data = _send_sms_nexmo(number, body)
    if not 200 <= status < 300:
        raise errors.ExternalError('Failed to send SMS')
