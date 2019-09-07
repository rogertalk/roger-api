# -*- coding: utf-8 -*-

import os
import os.path
import sys

from google.appengine.api import namespace_manager


lib_path = os.path.join(os.path.dirname(__file__), 'lib')

# Merge google libraries in `lib` with standard GAE ones.
import google
google.__path__.append(os.path.join(lib_path, 'google'))

# Add `lib` subdirectory to `sys.path` for third-party libraries.
sys.path.insert(0, lib_path)


# Import roger modules here since they depend on the lib path.
from roger import config, report
from roger_common import reporting
from roger_common.reporters import logging


def webapp_add_wsgi_middleware(app):
    if config.DEVELOPMENT:
        from roger_common import profiling
        #app = profiling.wsgi_middleware(app)
    return app


# Add reporters if we're not running unit tests.
if not os.environ.get('ROGER_SCRIPT'):
    if config.PRODUCTION:
        reporting.add_reporter(report.BatchedBigQueryReporter())
    reporting.add_reporter(logging)


# Put all data into a "dev" namespace to avoid breaking production.
def namespace_manager_default_namespace_for_request():
    return config.NAMESPACE
