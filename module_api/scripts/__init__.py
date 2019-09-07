# -*- coding: utf-8 -*-

import os
import os.path
import sys


SCRIPT_DIR = os.path.dirname(__file__)

OAUTH2_CLIENT_SECRET_PATH = os.path.join(SCRIPT_DIR, './oauth2_client_secret.json')
OAUTH2_CREDENTIALS_PATH = os.path.join(SCRIPT_DIR, './.oauth2_credentials.json')
# Keeping the same scopes across scripts simplifies things.
OAUTH2_SCOPES = [
    'https://www.googleapis.com/auth/bigquery',
]

PLAUSIBLE_SDK_PATHS = [
    # Google Cloud SDK in home directory
    '~/google-cloud-sdk/platform/google_appengine/',
    # Google App Engine Launcher inside Google Cloud SDK
    '~/google-cloud-sdk/bin/GoogleAppEngineLauncher.app/Contents/Resources/GoogleAppEngine-default.bundle/Contents/Resources/google_appengine/',
    # Homebrew + Caskroom installed Google Cloud SDK
    '/usr/local/Caskroom/google-cloud-sdk/latest/google-cloud-sdk/platform/google_appengine/',
    # Google App Engine Launcher in Applications
    '/Applications/GoogleAppEngineLauncher.app/Contents/Resources/GoogleAppEngine-default.bundle/Contents/Resources/google_appengine/',
]


class ScriptBase(object):
    def __init__(self):
        self.args = sys.argv[1:]
        # Attempt to discover the SDK path.
        for path in PLAUSIBLE_SDK_PATHS:
            path = os.path.expanduser(path)
            if os.path.exists(path):
                print 'Using SDK path:', path
                self.sdk_path = path
                break
        else:
            print 'WARNING: Failed to discover the Google App Engine SDK'
            self.sdk_path = None

    def before_setup(self):
        pass

    def main(self):
        pass

    def get_oauth2_credentials(self):
        from oauth2client.file import Storage
        from oauth2client.client import flow_from_clientsecrets
        from oauth2client import tools

        storage = Storage(OAUTH2_CREDENTIALS_PATH)
        credentials = storage.get()
        if credentials is None or credentials.invalid:
            flow = flow_from_clientsecrets(OAUTH2_CLIENT_SECRET_PATH,
                                           scope=OAUTH2_SCOPES)
            flags = tools.argparser.parse_args(self.args)
            credentials = tools.run_flow(flow, storage, flags)
        return credentials

    def run(self):
        os.environ['ROGER_SCRIPT'] = '1'
        self.before_setup()

        # Set up Google App Engine environment.
        sys.path.insert(0, self.sdk_path)
        import appengine_config
        import dev_appserver
        dev_appserver.fix_sys_path()

        self.main()
