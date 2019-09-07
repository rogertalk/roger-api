# -*- coding: utf-8 -*-

import json
import logging

from google.appengine.api import urlfetch

from roger_common import errors


def itunes(receipt_data, url='https://buy.itunes.apple.com/verifyReceipt'):
    try:
        result = urlfetch.fetch(
            url=url,
            method=urlfetch.POST,
            payload=json.dumps({'receipt-data': receipt_data}),
            deadline=30)
        data = json.loads(result.content)
    except Exception:
        logging.exception('Could not get result from Apple payment server.')
        raise errors.ServerError()
    if result.status_code != 200:
        logging.error('Apple payment server HTTP %d: %r', result.status_code, data)
        raise errors.InvalidArgument('Failed to validate receipt data with Apple')
    status = data.get('status')
    if not isinstance(status, (int, long)):
        logging.error('Could not get status: %r', data)
        raise errors.ServerError()
    if status == 0:
        return data
    elif status in (21000, 21002, 21003):
        raise errors.InvalidArgument('Invalid receipt data provided')
    elif status == 21005:
        raise errors.ExternalError()
    elif status == 21007:
        return itunes(receipt_data, url='https://sandbox.itunes.apple.com/verifyReceipt')
    elif status == 21008:
        return itunes(receipt_data)
    elif status == 21010:
        raise errors.InvalidArgument('Invalid purchase')
    elif 21100 <= status <= 21199:
        logging.error('Internal data access error: %r', data)
        raise errors.InvalidArgument('Invalid receipt data provided')
    logging.error('Unsupported status: %r', data)
    raise errors.NotSupported()
