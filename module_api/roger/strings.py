# -*- coding: utf-8 -*-

# TODO this entire file is probably deprecated

# Default format strings in case client doesn't support localization.
# The value should be a tuple of minimum API version and the format string.
PUSH_NOTIFICATIONS = {
    'NOTIFICATION_LISTENING': (9, u'\U0001f442 {} just listened to you'),
    'NOTIFICATION_LISTENING_UNKNOWN': (9, u'\U0001f442 Your friend just listened to you'),
    'NOTIFICATION_TALKING': (9, u'\U0001f600 {} is talking to you'),
    'NOTIFICATION_TALKING_GROUP': (9, u'\U0001f600 {} is talking to {}'),
    'NOTIFICATION_TALKING_UNKNOWN': (9, u'\U0001f600 Someone is talking to you'),
    'NOTIFICATION_TALKING_UNKNOWN_GROUP': (9, u'\U0001f600 Someone is talking to {}'),

    'NOTIFICATION_BUZZ': (11, u'\U0001F41D {} buzzed you'),
    'NOTIFICATION_BUZZ_GROUP': (11, u'\U0001F41D {} buzzed {}'),
    'NOTIFICATION_BUZZ_UNKNOWN': (11, u'\U0001F41D Someone buzzed you'),
    'NOTIFICATION_BUZZ_UNKNOWN_GROUP': (11, u'\U0001F41D Someone buzzed {}'),

    'NOTIFICATION_CHANGE_IMAGE': (99, u'\U0001F609 {} changed your conversation photo...'),

    'NOTIFICATION_TOP_TALKER': (99, u'\U0001F3C6 You ranked #{} on Roger’s top talkers this week! #TalkMore \U0001F389'),
}
