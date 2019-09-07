# -*- coding: utf-8 -*-

import json

from flask import Flask, request

from google.appengine.api import urlfetch


app = Flask(__name__)


@app.route('/_ah/queue/callbacks', methods=['POST'])
def callback():
    payload = json.loads(request.data)
    headers = {
        'Content-Type': 'application/json',
    }
    result = urlfetch.fetch(
        url=payload['callback_url'],
        method=urlfetch.POST,
        deadline=60,
        payload=json.dumps(payload['data']),
        headers=headers)
    return ''
