# -*- coding: utf-8 -*-

import logging

from flask import request

from roger import config
from roger_common import errors, flask_extras


def enforce_https():
    if not request.is_secure:
        return 'Try again with HTTPS.', 403


def set_up(app, **kwargs):
    """Add standard header and error handlers to an API endpoint."""
    app.config['DEBUG'] = config.DEVELOPMENT
    if not config.DEVELOPMENT:
        # Do not allow any requests that are not via HTTPS.
        app.before_request(enforce_https)
    # Ensure that the API endpoints are accessible by the app.
    app.after_request(flask_extras.add_cors_headers)

    @app.errorhandler(404)
    @flask_extras.json_service(**kwargs)
    def not_found(e):
        raise errors.UnsupportedEndpoint('The requested endpoint does not exist')

    @app.errorhandler(405)
    @flask_extras.json_service(**kwargs)
    def method_not_allowed(e):
        raise errors.MethodNotAllowed('That HTTP method is not supported')
