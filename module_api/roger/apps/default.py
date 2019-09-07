# -*- coding: utf-8 -*-

from flask import Flask

from roger import apps


# Set up an empty endpoint for handling unhandled requests to the API.
app = Flask(__name__)
apps.set_up(app)
