from datetime import timedelta
import os
import os.path

from roger_common import identifiers
from roger_common.config import *

PROJECT_ID = 'roger-api'
PROJECT_NUMBER = -1

# The namespace for data etc.
if PRODUCTION:
    NAMESPACE = None
else:
    NAMESPACE = 'dev'

MINIMUM_BUILD = {
    'Fika': 166,
    'ReactionCam': 282,
}

# Tags that are not visible via the API.
# TODO: Migrate these to all contain a whitespace.
INTERNAL_TAGS = {'flagged', 'is approved', 'is draft', 'is hidden', 'is hot', 'is reacted', 'onhold', 'published'}

# Tags that cannot be added by a user.
RESTRICTED_TAGS = INTERNAL_TAGS | {'featured', 'trending'}

# Tags that cannot be used for content list endpoints.
UNLISTED_TAGS = INTERNAL_TAGS | {'deleted', 'recording'}

# Tags that will not be copied from original to reaction.
NON_TRANSFERABLE_TAGS = RESTRICTED_TAGS | UNLISTED_TAGS | {'original', 'reacttothis', 'repost'}

# Account properties that a user has to pay for to unlock and change.
PREMIUM_PROPERTIES = {
    'record_hq': 150,
}

# The id of the wallet that receives all Coins spent on unlocks etc.
PURCHASES_WALLET_ID = 'account_5508037033852928'

# These content ids will be randomly injected as the first few requests.
ONBOARDING_REQUESTS_INJECT_IDS = [
    5669074366365696,  # Becky G - Mayores (Official Video) ft. Bad Bunny
    5308314775191552,  # Becky G, Natti Natasha - Sin Pijama (Official Video)
]

ONBOARDING_REQUESTS_CADENCE = lambda i: int(60*60*(24*(1+i//2) + i%2*0.5))
ONBOARDING_REQUESTS_COUNT = 10

SUGGESTION_POOL_SUBREDDITS = [
    'hiphopheads',
]

TOP_CREATOR_IDS = [
]

ITUNES_PRODUCTS = {
    'RCOINS80': {'active': True, 'type': 'currency', 'amount': 80},
    'RCOINS165': {'active': True, 'type': 'currency', 'amount': 165},
    'RCOINS420': {'active': True, 'type': 'currency', 'amount': 420},
    'RCOINS1750': {'active': True, 'type': 'currency', 'amount': 1750},
    # Inactive products.
    'RCOINS10': {'active': False, 'type': 'currency', 'amount': 10},
    'RCOINS21': {'active': False, 'type': 'currency', 'amount': 21},
    'RCOINS25': {'active': False, 'type': 'currency', 'amount': 25},
    'RCOINS55': {'active': False, 'type': 'currency', 'amount': 55},
}

CONTENT_CACHE_MARKER = '&q:$<_z/'

ANONYMOUS_ID = 5091062658891776L
REACTION_CAM_ID = 5508037033852928L

YOUTUBE_API_KEY = '_REMOVED_'

# Twitter credentials for reaction.cam tweets.
# If there's any reason to believe they're leaked, revoke at:
# https://apps.twitter.com/app/_REMOVED_/keys
TWITTER_CONSUMER_KEY        = '_REMOVED_'
TWITTER_CONSUMER_SECRET     = '_REMOVED_'
TWITTER_ACCESS_TOKEN_KEY    = '_REMOVED_'
TWITTER_ACCESS_TOKEN_SECRET = '_REMOVED_'

# Task queues.
AUDIO_QUEUE_NAME = 'jobs'
BIGQUERY_QUEUE_NAME = 'bigquery-reporting'
BIGQUERY_CRON_QUEUE_NAME = 'jobs'
BOTS_QUEUE_NAME = 'bots'
CALLBACKS_QUEUE_NAME = 'bots'
DELETE_CHUNKS_QUEUE_NAME = 'jobs'
INTERNAL_QUEUE = 'jobs'
LOCATION_QUEUE_NAME = 'jobs'
SERVICE_QUEUE_NAME = 'jobs'
TOP_TALKER_QUEUE_NAME = 'jobs'

if DEVELOPMENT:
    AUDIO_QUEUE_NAME = 'default'
    BIGQUERY_QUEUE_NAME = 'default'
    BIGQUERY_CRON_QUEUE_NAME = 'default'
    BOTS_QUEUE_NAME = 'default'
    CALLBACKS_QUEUE_NAME = 'default'
    DELETE_CHUNKS_QUEUE_NAME = 'default'
    INTERNAL_QUEUE = 'default'
    LOCATION_QUEUE_NAME = 'default'
    NOTIFICATIONS_QUEUE_NAME = 'default'
    SERVICE_QUEUE_NAME = 'default'
    TOP_TALKER_QUEUE_NAME = 'default'

VALID_NOTIF_PLATFORMS = (
    'gcm',
    'gcm_ios',
    'ios',
    'pushkit',
)

SERVICE_HLS_TRANSCODE = 'https://_REMOVED_/v2/transcode'
SERVICE_PUSH = 'http://_REMOVED_/v1/push'
SERVICE_THUMBNAIL = 'https://_REMOVED_.cloudfront.net/v1/thumbnail'

# The max number of device tokens per user.
MAX_DEVICE_TOKENS = 6

# Accounts that get a predetermined code when logging in.
DEMO_ACCOUNTS = {
    'email:apple.com/demo',
    '+15556727753',
    '+14325550101',
    '+14325550102',
}

ALEXA_SKILL_SECRET = '_REMOVED_'

# Amazon Web Services API.
if PRODUCTION:
    AWS_SECRETS_FILE = os.path.join(os.path.dirname(__file__), '../secrets/dev_aws.json')
else:
    AWS_SECRETS_FILE = os.path.join(os.path.dirname(__file__), '../secrets/dev_aws.json')

# Google Cloud Messaging configuration
GCM_API_URL = 'https://gcm-http.googleapis.com/gcm/send'
GCM_API_KEY = '_REMOVED_'

WEATHER_BOT_ID = -1

# Google Maps API.
GOOGLE_MAPS_API_KEY = '_REMOVED_'

# IFTTT configuration.
IFTTT_CHANNEL_KEY = '_REMOVED_'

# BigQuery related configuration.
BIGQUERY_PROJECT = 'roger-api'
BIGQUERY_LEASE_AMOUNT = 500  # The number of events to lease at a time.
BIGQUERY_LEASE_TIME = timedelta(minutes=10)  # The time to lease a batch.
if PRODUCTION:
    BIGQUERY_DATASET = 'roger_reporting'
else:
    BIGQUERY_DATASET = 'roger_reporting_dev'

CLAIMABLE_IDENTIFIER_TYPES = {
    identifiers.EMAIL,
    identifiers.PHONE,
    identifiers.SERVICE_ID,
    identifiers.USERNAME,
}

DO_NOT_UPLOAD_TO_OFFICIAL_YOUTUBE = [
    5627635649478656,  # @pinkmetalhead
]

INVALID_IDENTIFIERS = {
    'about', 'account', 'admin', 'auth', 'beta', 'blog', 'ceo', 'dw', 'embed', 'featured',
    'feedback', 'forward', 'fw', 'get', 'group', 'hello', 'help', 'join', 'legal',
    'login', 'me', 'policies', 'press', 'privacy', 'profile', 'register', 'services',
    'settings', 'status', 'stream', 'support', 'team',
}

# Default avatars. One will be picked at random. Max: 256 values.
DEFAULT_ACCOUNT_IMAGES = [
]

# Chunk/stream settings.
CHUNK_MAX_AGE = timedelta(days=7)

# Challenge settings.
CHALLENGE_CODE_LENGTH = 6
CHALLENGE_MAX_TRIES = 5
CHALLENGE_TTL = timedelta(hours=24)

# Rate limiting to prevent abuse.
# key: (bucket_size, refill_per_second)
RATELIMITS = {
    'challenge:call_code': (5, 0.1),
    'challenge:email_code': (100, 5),
    'challenge:ip:*': (20, 0.001),
    'challenge:sms_code': (100, 5),
    'default': (100, 10),
}

# Request throttling (for very expensive requests).
THROTTLE_CHALLENGE = 90
THROTTLE_CHALLENGE_CALL = 3600
THROTTLE_DOWNLOAD_LINK = 300
THROTTLE_INVITE = 300

# Authorization code settings.
# TODO: Sanity check expiry time.
AUTH_CODE_LENGTH = 16
AUTH_CODE_TTL = timedelta(minutes=5)

# Login token settings.
LOGIN_TOKEN_LENGTH = 8
LOGIN_TOKEN_TTL = timedelta(hours=24)

# URL formats.
if PRODUCTION:
    API_HOST = 'https://api.rogertalk.com'
    WEB_HOST = 'https://rogertalk.com'
    LOGIN_TOKEN_URL_FORMAT = 'http://rgr.im/-%s'
else:
    API_HOST = 'https://dev-dot-roger-api.appspot.com'
    WEB_HOST = 'https://dev-dot-roger-web-client.appspot.com'
    LOGIN_TOKEN_URL_FORMAT = WEB_HOST + '/-%s'
FILE_URL_FORMAT = API_HOST + '/file/%s'

# Storage.
STORAGE_URL_HOST = 'https://storage.googleapis.com/'
STORAGE_URL_FORMAT = STORAGE_URL_HOST + '%s/%s'

STORAGE_LEGACY_CDN = 'https://_REMOVED_.cloudfront.net/'
STORAGE_LEGACY_ORIGINS = [
    'https://storage.googleapis.com/_REMOVED_/',
]

BUCKET_PERSISTENT = 'roger-api-persistent'
BUCKET_SHORTLIVED = 'roger-api-shortlived'
STORAGE_PATH_PERSISTENT = '/%s' % (BUCKET_PERSISTENT,)
STORAGE_PATH_SHORTLIVED = '/%s' % (BUCKET_SHORTLIVED,)

S3_BUCKET = 'reaction.cam'
S3_BUCKET_CDN = 'https://_REMOVED_.cloudfront.net/'

# SMS settings.
if PRODUCTION:
    SMS_MESSAGEBIRD_API_TOKEN = '_REMOVED_'
    SMS_NEXMO_API_ACCOUNT = '_REMOVED_'
    SMS_NEXMO_API_TOKEN = '_REMOVED_'
    SMS_TWILIO_API_ACCOUNT = '_REMOVED_'
    SMS_TWILIO_API_TOKEN = '_REMOVED_'
    SMS_TWILIO_API_NUMBER = '+14427776437'  # +1 (442) 77 ROGER!
else:
    SMS_MESSAGEBIRD_API_TOKEN = '_REMOVED_'
    SMS_NEXMO_API_ACCOUNT = ''
    SMS_NEXMO_API_TOKEN = ''
    SMS_TWILIO_API_ACCOUNT = '_REMOVED_'
    SMS_TWILIO_API_TOKEN = '_REMOVED_'
    SMS_TWILIO_API_NUMBER = '+15005550006'  # Not a real number, used in testing.

CALL_NEXMO_FROM_NUMBER = '13477733551'

# The header for implicitly updating latitude, longitude coordinates.
LOCATION_HEADER = 'X-Roger-LatLng'

# Weather API.
FORECASTIO_API_KEY = '_REMOVED_'

# Email settings.
# DEPRECATED: We use SendGrid instead.
EMAIL_DOMAIN = 'roger-api.appspotmail.com'
# Used to send emails through SendGrid.
SENDGRID_API_KEY = '_REMOVED_'

# Slack settings.
SLACK_CLIENT_ID = '_REMOVED_'
SLACK_CLIENT_SECRET = '_REMOVED_'
SLACK_WEBHOOK_URLS = {
    'fika': ('https://hooks.slack.com/services/_REMOVED_/_REMOVED_/'
             '_REMOVED_'),
    'reactioncam': ('https://hooks.slack.com/services/_REMOVED_/_REMOVED_/'
                    '_REMOVED_'),
    'review': ('https://hooks.slack.com/services/_REMOVED_/_REMOVED_/'
               '_REMOVED_'),
    'roger': ('https://hooks.slack.com/services/_REMOVED_/_REMOVED_/'
              '_REMOVED_'),
}

# Key used for encrypting sessions.
# TODO: Move this into some kind of opaque config store.
if PRODUCTION:
    ENCRYPTION_KEY = '_REMOVED_'
else:
    ENCRYPTION_KEY = '_REMOVED_'

WEB_ENCRYPTION_KEY = ('00000000000000000000000000000000'
                      '00000000000000000000000000000000').decode('hex')
SLACK_ENCRYPTION_KEY = '_REMOVED_'

# Codes that will let new users skip the waiting page. Describe the use case in value.
# Please use separate codes so that it's easy to disable specific backdoors.
if PRODUCTION:
    BACKDOOR_CODES = {
        '5HWw033wWV': 'usertesting.com (2015-05-07)',
        'VpwhLkvugT': 'user registered via a personal page',
        'QhRzPfllN3': None,
        '2at1VbyQI3': None,
        'Jnkw5UIGBC': None,
        'UyT6n0Sw9u': None,
        'tQ98LdkWF4': None,
        'QUKYp7nlWx': None,
        'x12WYFIadw': None,
        'RJU98trk7M': None,
    }
else:
    BACKDOOR_CODES = {
        'letmein': 'Development server',
    }

# Valid statuses for accounts. Status may only change down the list, not up.
VALID_STATUS_TRANSITIONS = [
    ['temporary'],
    ['requested'],
    ['invited'],
    ['waiting'],
    ['voicemail'],
    ['unclaimed', 'active', 'inactive', 'bot', 'employee', 'banned', 'deleted'],
]

# TODO: Use a transition table like this:
#VALID_STATUS_TRANSITIONS = {
#    'temporary': ['invited', 'requested', 'waiting', 'voicemail', 'active'],
#    'requested': ['active', 'waiting'],
#    'invited': ['active', 'waiting'],
#    'voicemail': ['active', 'waiting'],
#    'waiting': ['active'],
#    'active': ['inactive', 'employee'],
#    'inactive': ['active'],
#    'bot': [],
#    'employee': ['active'],
#}

# Common e-mail domains that aren't used for internal company communication.
PUBLIC_EMAIL_DOMAINS = {
    'aim.com',
    'aol.com',
    'arnet.com.ar',
    'att.net',
    'bellsouth.net',
    'blueyonder.co.uk',
    'bol.com.br',
    'bt.com',
    'btinternet.com',
    'charter.net',
    'comcast.net',
    'cox.net',
    'daum.net',
    'earthlink.net',
    'email.com',
    'embarqmail.com',
    'facebook.com',
    'fastmail.fm',
    'fibertel.com.ar',
    'free.fr',
    'freeserve.co.uk',
    'games.com',
    'globo.com',
    'globomail.com',
    'gmail.com',
    'gmx.com',
    'gmx.de',
    'gmx.fr',
    'gmx.net',
    'google.com',
    'googlemail.com',
    'hanmail.net',
    'hotmail.be',
    'hotmail.ca',
    'hotmail.co.jp',
    'hotmail.co.uk',
    'hotmail.com',
    'hotmail.com.ar',
    'hotmail.com.br',
    'hotmail.com.mx',
    'hotmail.com.tr',
    'hotmail.de',
    'hotmail.es',
    'hotmail.fr',
    'hotmail.it',
    'hotmail.no',
    'hotmail.ph',
    'hush.com',
    'hushmail.com',
    'icloud.com',
    'ig.com.br',
    'inbox.com',
    'itelefonica.com.br',
    'juno.com',
    'laposte.net',
    'lavabit.com',
    'list.ru',
    'live.be',
    'live.ca',
    'live.co.uk',
    'live.co.za',
    'live.com',
    'live.com.ar',
    'live.com.au',
    'live.com.mx',
    'live.com.pt',
    'live.de',
    'live.fr',
    'live.it',
    'live.se',
    'love.com',
    'mac.com',
    'mail.com',
    'mail.ru',
    'mailinator.com',
    'me.com',
    'msn.com',
    'nate.com',
    'naver.com',
    'neuf.fr',
    'ntlworld.com',
    'o2.co.uk',
    'oi.com.br'
    'online.de',
    'orange.fr',
    'orange.net',
    'outlook.com',
    'outlook.com.br',
    'outlook.de',
    'outlook.es',
    'outlook.in',
    'outlook.it',
    'outlook.sa',
    'pacbell.net',
    'pobox.com',
    'prodigy.net.mx',
    'protonmail.ch',
    'protonmail.com',
    'qq.com',
    'r7.com',
    'rambler.ru',
    'rocketmail.com',
    'rogers.com',
    'safe-mail.net',
    'samobile.net',
    'sapo.pt',
    'sbcglobal.net',
    'sfr.fr',
    'shaw.ca',
    'sina.com',
    'sky.com',
    'skynet.be',
    'speedy.com.ar',
    't-online.de',
    'talk21.com',
    'talktalk.co.uk',
    'tds.net',
    'telenet.be',
    'terra.com.br',
    'tiscali.co.uk',
    'tvcablenet.be',
    'twc.com',
    'twcny.rr.com',
    'uol.com.br',
    'verizon.net',
    'virgin.net',
    'virginmedia.com',
    'voo.be',
    'wanadoo.co.uk',
    'wanadoo.fr',
    'web.de',
    'wow.com',
    'xtra.co.nz',
    'ya.ru',
    'yahoo.ca',
    'yahoo.co.id',
    'yahoo.co.in',
    'yahoo.co.jp',
    'yahoo.co.kr',
    'yahoo.co.nz',
    'yahoo.co.uk',
    'yahoo.co.za',
    'yahoo.com',
    'yahoo.com.ar',
    'yahoo.com.au',
    'yahoo.com.br',
    'yahoo.com.hk',
    'yahoo.com.mx',
    'yahoo.com.ph',
    'yahoo.com.sg',
    'yahoo.com.tr',
    'yahoo.com.tw',
    'yahoo.de',
    'yahoo.es',
    'yahoo.fr',
    'yahoo.gr',
    'yahoo.ie',
    'yahoo.in',
    'yahoo.it',
    'yahoo.se',
    'yandex.com',
    'yandex.ru',
    'ygm.com',
    'ymail.com',
    'zipmail.com.br',
    'zoho.com',
}
