#!/usr/local/bin/python
# -*- coding: utf-8 -*-
#
# cat numbers | ./sms_android_beta.py

import base64
import sys

sys.path.append('/lib/python2.7/site-packages/')

import requests


SMS_NEXMO_API_ACCOUNT = '_REMOVED_'
SMS_NEXMO_API_TOKEN = '_REMOVED_'
SMS_TWILIO_API_ACCOUNT = '_REMOVED_'
SMS_TWILIO_API_TOKEN = '_REMOVED_'
SMS_TWILIO_API_NUMBER = '+14427776437'


def send_sms(number, link):
    body = 'Hello, you recently signed up for a Roger Beta Invite, and here it is:\n\n%s \n\n- The Roger Team' % link
    if number.startswith('+1'):
        # Twilio
        method = requests.post
        url = 'https://api.twilio.com/2010-04-01/Accounts/%s/Messages.json' % SMS_TWILIO_API_ACCOUNT
        params = dict()
        data = {
            'From': SMS_TWILIO_API_NUMBER,
            'To': number,
            'Body': body.encode('utf8'),
            }
        headers = {
            'Authorization': 'Basic %s' % base64.b64encode('%s:%s' % (SMS_TWILIO_API_ACCOUNT, SMS_TWILIO_API_TOKEN)),
            'Content-Type': 'application/x-www-form-urlencoded',
            }
    else:
        # Nexmo
        method = requests.get
        url = 'https://rest.nexmo.com/sms/json'
        params = {
            'api_key': SMS_NEXMO_API_ACCOUNT,
            'api_secret': SMS_NEXMO_API_TOKEN,
            'from': 'Roger',
            'to': number[1:],
            'text': body,
        }
        data = dict()
        headers = dict()
    try:
        result = method(url, params=params, data=data, headers=headers)
    except Exception as e:
        print 'Failing number: %r' % number
        return -1, {'code': -1, 'message': str(e)}
    if not 200 <= result.status_code < 300:
        print 'API error %d: %s %s' % (result.status_code, number, result.json())
    else:
        print 'Sent to %s' % number


if __name__ == '__main__':
    for number in sys.stdin:
        send_sms(number[:-1], 'https://rogertalk.com/get')
