# -*- coding: utf-8 -*-

import logging
import mimetypes

from StringIO import StringIO

import cloudstorage
from flask import Flask, send_file

from roger import apps, files


app = Flask(__name__)
apps.set_up(app)


@app.route('/file/<filename>', methods=['GET'])
def get_file(filename):
    return 'Not found.', 404


@app.route('/image/<filename>', methods=['GET'])
def get_image(filename):
    return 'Not found.', 404
