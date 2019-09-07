# -*- coding: utf-8 -*-

import base64
from datetime import datetime, timedelta
import flask
import fnmatch
import json
import logging
import mimetypes
import os
import pytz
import textwrap

from google.appengine.ext import deferred

import sendgrid
from sendgrid.helpers import mail

from roger import config
from roger_common import random


_sendgrid_api = sendgrid.SendGridAPIClient(apikey=config.SENDGRID_API_KEY)


COUNTRY_GREETING_LIST = [
    ['cdc5371ad395bd29d5b68fa3462e100d70a5b301d850b490254bbf322550dcd0-p.mp3', 10744, ['BR']],
]


COUNTRY_PREFIXES = {
    'AD': ('+376',),
    'AO': ('+244',),
    'AR': ('+54',),
    'BO': ('+591',),
    'BR': ('+55',),
    'CV': ('+238',),
    'CL': ('+56',),
    'CO': ('+57',),
    'CR': ('+506',),
    'CU': ('+53',),
    'DO': ('+1809', '+1829', '+1849',),
    'EC': ('+593',),
    'SV': ('+503',),
    'GT': ('+502',),
    'MX': ('+52',),
    'MZ': ('+258',),
    'NI': ('+505',),
    'PA': ('+507',),
    'PE': ('+51',),
    'PT': ('+351',),
    'PR': ('+1787', '+1939',),
    'ST': ('+239',),
    'ES': ('+34',),
    'UY': ('+598',),
    'VE': ('+58',),
}


LANGUAGE_COUNTRIES = {
    'es-MX': ('AD', 'AR', 'AR', 'CL', 'CO', 'CR', 'CU', 'DO', 'EC', 'SV', 'GT', 'MX', 'NI', 'PA', 'PE', 'PR', 'ES', 'UY', 'VE'),
    'pt-BR': ('AO', 'BR', 'CV', 'MZ', 'PT', 'ST'),
}


# TODO: Localization.
EMAIL_APP_SETTINGS = {
    'fika': {
        'html': open('strings/fika_email.html').read().decode('utf-8'),
        'from_email': 'hello@fika.io',
        'default_from_name': 'fika.io',
        'templates': {
            'generic_invite': {
                'subject': u'🎉 Congratulations %(invited_name)s, your fika.io pass is here',

                'html_body': u"""\
                <p>Welcome <strong style="font-weight:600;">%(invited_name)s</strong>!</p>

                <p>We're only letting a small, exclusive set of individuals into
                <a href="https://fika.io/">fika.io</a> at this time, and you've made the cut. ✂️</p>

                <p style="text-align:center;">
                    <a class="button" href="https://fika.io/get">Join Now</a>
                </p>

                <p>Reply to this email if you have any questions!<p>

                <p>- fika.io team</p>

                <p style="font-size:10px;text-align:center;">Open
                <a href="https://fika.io/get">https://fika.io/get</a>
                if the button above won't work.</p>
                """,

                'text_body': u"""\
                Welcome %(invited_name)s!

                We're only letting a small, exclusive set of individuals into fika.io
                at this time, and you've made the cut. Join now:

                https://fika.io/get
                (open on your iPhone)

                Reply to this email if you have any questions.

                - fika.io team
                """,
            },
            'invite': {
                'subject': u'🎉 %(invited_name)s, you\'re invited to join me on fika.io',

                'html_body': u"""\
                <p>Hello <strong style="font-weight:600;">%(invited_name)s</strong>,</p>

                <p>%(inviter_name)s (cc'ed) invited you to join
                <a href="https://fika.io/">fika.io</a>, a collaboration platform that
                makes you productive anywhere, even your ride to the office.</p>

                <p style="text-align:center;">
                    <a class="button" href="https://fika.io/get">Get Started</a>
                </p>

                <p>Reply to this email if you have any questions.<p>

                <p>- fika.io team</p>

                <p style="font-size:10px;text-align:center;">Open
                <a href="https://fika.io/get">https://fika.io/get</a>
                if the button above won't work.</p>
                """,

                'text_body': u"""\
                Hello %(invited_name)s,

                You've been invited by %(inviter_name)s to join fika.io. Accept invitation:

                https://fika.io/get
                (open on your iPhone)

                Reply to this email if you have any questions.

                - fika.io team
                """,
            },
            'login': {
                'subject': u'%(code)s is your fika.io sign in code',

                'html_body': u"""\
                <p>You requested a code to sign into fika.io, just enter the one below in the app
                and you're good to go.</p>
                <p><span class="code">%(code)s</span></p>
                <p>- fika.io team</p>
                <p>PS: don't forget to <a href="https://fika.io/get">get the app</a> on your
                iPhone 😉</p>
                """,

                'text_body': u"""\
                You requested a code to sign into fika.io, just enter the one below in the app and
                you're good to go!

                Code: %(code)s

                - fika.io team

                PS: don't forget to get the app for your iPhone at https://fika.io/get
                """,
            },
            'login_pdf': {
                'subject': u'Your fika.io pass',

                'html_body': u"""\
                <p><b style="font-size:18px;">Welcome to fika.io!</b></p>
                <p>Open the attached PDF document and follow the instructions to complete set up. Enjoy!</p>
                <p>- The fika.io team</p>
                <p>P.S.: If that doesn’t work, simply enter the code %(code)s in the app and you're good to go.</p>
                """,

                'text_body': u"""\
                You requested a code to sign into fika.io, just enter the one below in the app and
                you're good to go!

                Code: %(code)s

                - fika.io team

                PS: don't forget to get the app for your iPhone at https://fika.io/get
                """,
            },
            'unplayed_chunk': {
                'subject': u'%(destination_sentence)s on fika.io',

                'html_body': u"""\
                <table>
                    <tr>
                        <td valign="top"><a href="%(watch_url)s"><img src="%(thumb)s" style="border-radius:4px;" width="80" height="80"></a></td>
                        <td style="padding:0 0 20px 6px;">
                            <b>%(sender_name)s shared a video at %(time)s.</b><br><br>
                            <a href="%(watch_url)s">Watch the video »</a>
                        </td>
                    </tr>
                </table>
                """,

                'text_body': u"""\
                %(sender_name)s shared a video at %(time)s.

                Watch the video: %(watch_url)s
                """
            },
            'unplayed_chunk_transcript': {
                'subject': u'%(destination_sentence)s on fika.io',

                'html_body': u"""\
                <table>
                    <tr>
                        <td valign="top"><a href="%(watch_url)s"><img src="%(thumb)s" style="border-radius:4px;" width="80" height="80"></a></td>
                        <td style="padding:0 0 20px 6px;">
                            <b style="font-size:18px;">%(sender_name)s at %(time)s:</b><br>
                            %(transcript)s<br><br>
                            <a href="%(watch_url)s">Watch the video »</a>
                        </td>
                    </tr>
                </table>
                """,

                'text_body': u"""\
                %(sender_name)s at %(time)s:
                %(transcript)s

                Watch the video: %(watch_url)s
                """
            },
            'welcome_1': {
                'subject': u'Welcome to fika.io!',

                'html_body': u"""\
                <p>Hey there!</p>
                <p>We’re thrilled to have you on fika.io!</p>
                <p>To get you started, here’s a quick guide to help you <b>share your thoughts</b>
                and <b>annotate like a champion</b>:</p>
                <p><b style="font-size:18px;">Share your first screen cast</b></p>
                <ol>
                  <li>Hit “<b>Import to fika.io</b>” on the document you would like to annotate or share<br>
                  <img src="https://c.fika.io/s/welcome_1_1.gif"></li>
                  <li>Now go ahead and show what you mean, with <b>video, voice, and actions</b>! Don’t forget to
                  hit the record button!<br>
                  <img src="https://c.fika.io/s/welcome_1_2.gif"></li>
                  <li><b>Share it anywhere</b> you want<br>
                  <img src="https://c.fika.io/s/welcome_1_3.gif"></li>
                </ol>
                <p>Now that you know the secret, why don’t you go ahead and try it. And if you feel the urge to
                tell someone about this, we won’t blame you 😉</p>
                <p>Do let us know if you need any help. Enjoy!</p>
                <p>- The fika.io team</p>
                """,

                'text_body': u"""\
                Hey there!

                We’re thrilled to have you on fika.io!

                To get you started, here’s a quick guide to help you share your thoughts
                and annotate like a champion:

                Share your first screen cast

                  Hit “Import to fika.io” on the document you would like to annotate or share
                  https://c.fika.io/s/welcome_1_1.gif

                  Now go ahead and show what you mean, with video, voice, and
                  actions! Don’t forget to hit the record button!
                  https://c.fika.io/s/welcome_1_2.gif

                  Share it anywhere you want
                  https://c.fika.io/s/welcome_1_3.gif

                Now that you know the secret, why don’t you go ahead and try it.
                And if you feel the urge to tell someone about this, we won’t
                blame you. ;)

                Do let us know if you need any help. Enjoy!

                - The fika.io team
                """
            },
            'welcome_2': {
                'subject': u'[Adding Members] Time to gather your forces',

                'html_body': u"""\
                <p>Hey,</p>
                <p>We hope you managed to open your first document and annotated on it <b>like a boss</b>! Now,
                time to gather your forces on fika.io so that everyone can <b>be productive together</b>.</p>
                <p>Just swipe to the right on the app and you’ll be brought
                to the Timeline, then just click the options button:</p>
                <p><img src="https://c.fika.io/s/welcome_2_1.png" width="562" height="397"></p>
                <p>You can add people by <b>Email</b> or <b>Connect to your existing Slack Channels</b>:</p>
                <p><img src="https://c.fika.io/s/welcome_2_2.png" width="225" height="400"></p>
                <p>Go ahead, you can connect to as many people as you’d like! Enjoy.</p>
                <p>- The fika.io team</p>
                """,

                'text_body': u"""\
                Hey,

                We hope you managed to open your first document and annotated on it like a boss! Now,
                time to gather your forces on fika.io so that everyone can be productive together.

                Just swipe to the right on the app and you’ll be brought
                to the Timeline, then just click the options button:
                https://c.fika.io/s/welcome_2_1.png

                You can add people by Email or Connect to your existing Slack Channels:
                https://c.fika.io/s/welcome_2_2.png

                Go ahead, you can connect to as many people as you’d like! Enjoy.

                - The fika.io team
                """
            },
            'welcome_3': {
                'subject': u'[Annotations] Show what you mean',

                'html_body': u"""\
                <p>Hey,</p>
                <p>Hope you’re having a great day! We shall make it even better
                with this tip to help you <b>Annotate with ease</b>.</p>
                <p>Begin your annotation before or while you are recording:</p>
                <p><img src="https://c.fika.io/s/welcome_3_1.gif"></p>
                <p>Did you know you can also <b>switch ink colors</b>? 😎</p>
                <p><img src="https://c.fika.io/s/welcome_3_2.gif"></p>
                <p>Lastly, include text if you need:</p>
                <p><img src="https://c.fika.io/s/welcome_3_3.gif"></p>
                <p>Now nothing stands in your way to simply show what you really mean. Enjoy!</p>
                <p>- The fika.io team</p>
                """,

                'text_body': u"""\
                Hey,

                Hope you’re having a great day! We shall make it even better
                with this tip to help you Annotate with ease.

                Begin your annotation before or while you are recording:
                https://c.fika.io/s/welcome_3_1.gif

                Did you know you can also switch ink colors? 😎
                https://c.fika.io/s/welcome_3_2.gif

                Lastly, include text if you need:
                https://c.fika.io/s/welcome_3_3.gif

                Now nothing stands in your way to simply show what you really mean. Enjoy!

                - The fika.io team
                """
            },
            'welcome_4': {
                'subject': u'[Sharing] We love Slack, too',

                'html_body': u"""\
                <p>Hey,</p>
                <p>We know Slack is a great tool to communicate with your team, that’s
                why we made it seamless to share directly to your channels.</p>
                <p>You’ve learnt to record and annotate with fika.io,
                but to get your point across you have to share.</p>
                <p><b style="font-size:18px;">Share to Slack #</b></p>
                <ol>
                    <li>Make sure you’ve added your Slack channels in fika.io (click
                    <a href="https://watch.fika.io/-/YfcaW8nwgjNJ64gCU4x8f">here</a>
                    if you forgot how to)</li>
                    <li>Go ahead and use fika.io with your files or just record a video message</li>
                    <li>Your channel should appear on the list for you to share instantly:</li>
                </ol>
                <p><img src="https://c.fika.io/s/welcome_4_1.png" width="562" height="397"></p>
                <p>Now you don’t have to worry about your workflow being all over the place! 👍🏼 Enjoy.</p>
                <p>- The fika.io team</p>
                """,

                'text_body': u"""\
                Hey,

                We know Slack is a great tool to communicate with your team, that’s
                why we made it seamless to share directly to your channels.

                You’ve learnt to record and annotate with fika.io,
                but to get your point across you have to share.

                Share to Slack #

                    1. Make sure you’ve added your Slack channels in fika.io
                    2. Go ahead and use fika.io with your files or just record a video message
                    3. Your channel should appear on the list for you to share instantly:
                       https://c.fika.io/s/welcome_4_1.png

                Now you don’t have to worry about your workflow being all over the place! Enjoy.

                - The fika.io team
                """
            },
            'welcome_5': {
                'subject': u'[Sharing] Everywhere is possible',

                'html_body': u"""\
                <p>Hey,</p>
                <p>We’re going to give you the next sharing tip! This time its
                not only to Slack, but to <b><u>anywhere</u></b>!</p>
                <p><b style="font-size:18px;">Share Everywhere &amp; Anywhere</b></p>
                <ol>
                  <li>Once you are done recording, there will be a link automatically generated:<br>
                  <img src="https://c.fika.io/s/welcome_5_1.png" width="400" height="300"></li>
                  <li>With the link copied into your clipboard, you can go ahead and paste it anywhere you want!<br>
                  <img src="https://c.fika.io/s/welcome_5_2.png" width="373" height="248"></li>
                  <li>Or <b>even better</b> do it through <b>fika.io</b>:<br>
                  <img src="https://c.fika.io/s/welcome_5_3.gif"></li>
                </ol>
                <p>Have a great one! Enjoy.</p>
                <p>- The fika.io team</p>
                """,

                'text_body': u"""\
                Hey,

                We’re going to give you the next sharing tip! This time its
                not only to Slack, but to anywhere!

                Share Everywhere & Anywhere

                  Once you are done recording, there will be a link automatically generated:
                  https://c.fika.io/s/welcome_5_1.png

                  With the link copied into your clipboard, you can go ahead and paste it anywhere you want!
                  https://c.fika.io/s/welcome_5_2.png

                  Or even better do it through fika.io:
                  https://c.fika.io/s/welcome_5_3.gif

                Have a great one! Enjoy.

                - The fika.io team
                """
            },
            'welcome_6': {
                'subject': u'[Opening Web Links] Shortcut to keep it streamlined',

                'html_body': u"""\
                <p>Hey,</p>
                <p>We all know shortcuts can make life so <b>much more efficient</b>. That’s why we’ve
                integrated some of the <b>best tools</b> you usually use as a shortcut in fika.io!</p>
                <p><b style="font-size:18px;">Open Web Links</b></p>
                <ol>
                  <li>In the app, just hit the little cloud icon at the bottom.</li>
                  <li>Search any link you want to show:<br>
                  <img src="https://c.fika.io/s/welcome_6_2.gif"></li>
                  <li>There are shortcuts that launch you straight into <b>Box</b>,
                  <b>Dropbox</b>, and <b>Google Drive</b> too:<br>
                  <img src="https://c.fika.io/s/welcome_6_3.gif"></li>
                </ol>
                <p>Believe us, we’re doing our best to develop more integrations
                and shortcuts for you! In the meantime, enjoy 😃</p>
                <p>- The fika.io team</p>
                """,

                'text_body': u"""\
                Hey,

                We all know shortcuts can make life so much more efficient. That’s why we’ve
                integrated some of the best tools you usually use as a shortcut in fika.io!

                Open Web Links

                  In the app, just hit the little cloud icon at the bottom.
                  Search any link you want to show:
                  https://c.fika.io/s/welcome_6_2.gif

                  There are shortcuts that launch you straight into Box,
                  Dropbox, and Google Drive too:
                  https://c.fika.io/s/welcome_6_3.gif

                Believe us, we’re doing our best to develop more integrations
                and shortcuts for you! In the meantime, enjoy.

                - The fika.io team
                """
            },
            'welcome_7': {
                'subject': u'[Finale] Pro-tip on fika.io’s interface',

                'html_body': u"""\
                <p>Hey,</p>
                <p>We know... it is sad that this is the final fika.io tip for you. By now
                you should be a pro with your new effective workflow, <b>opening</b>,
                <b>creating</b>, <b>and sharing</b> at your fingertips!</p>
                <p>To wrap it up, we’ll let you know our <b>final tip</b> for you to take
                control of your fika.io experience.</p>
                <p><b style="font-size:18px;">Controlling your playback</b></p>
                <ul>
                  <li>Pause the playback by <b>pressing and holding</b> on your screen:<br>
                  <img src="https://c.fika.io/s/welcome_7_1.gif"></li>
                  <li>Go back and forth with the <b>scrubber</b>:<br>
                  <img src="https://c.fika.io/s/welcome_7_2.gif"></li>
                  <li><b>Adjust the speed</b> of playback:<br>
                  <img src="https://c.fika.io/s/welcome_7_3.gif"></li>
                </ul>
                <p>Do let us know if you have any questions and we’ll
                be happy to answer them for you. Enjoy!</p>
                <p>- The fika.io team</p>
                """,

                'text_body': u"""\
                Hey,

                We know... it is sad that this is the final fika.io tip for you. By now
                you should be a pro with your new effective workflow, opening,
                creating, and sharing at your fingertips!

                To wrap it up, we’ll let you know our final tip for you to take
                control of your fika.io experience.

                Controlling your playback

                  Pause the playback by pressing and holding on your screen:
                  https://c.fika.io/s/welcome_7_1.gif

                  Go back and forth with the scrubber:
                  https://c.fika.io/s/welcome_7_2.gif

                  Adjust the speed of playback:
                  https://c.fika.io/s/welcome_7_3.gif

                Do let us know if you have any questions and we’ll
                be happy to answer them for you. Enjoy!

                - The fika.io team
                """
            },
        },
    },
    'reactioncam': {
        'html': open('strings/reactioncam_email.html').read().decode('utf-8'),
        'from_email': 'yo@reaction.cam',
        'default_from_name': 'REACTION.CAM',
        'templates': {
            'artist_analytics': {
                'subject': 'Reaction.cam - Report %(report_date)s',

                'html_body': u"""\
                <p>Hi there!</p>

                <p>You’ve got <b>%(click_count)s new clicks</b> to <a href="%(release_url)s">your
                reaction request</a> and <b>%(reaction_count)s new reactions</b> today.</p>

                <p>Remember that you will not get reactions unless you share your request with your
                followers and ask them to react: <a href="%(release_url)s">%(release_url)s</a></p>

                <p>- Reaction.cam Artists Team</p>
                """,

                'text_body': u"""\
                Hi there!

                You’ve got %(click_count)s new clicks to your reaction
                request and %(reaction_count)s new reactions today.

                Remember that you will not get reactions unless you share
                your request with your followers and ask them to react:
                %(release_url)s

                - Reaction.cam Artists Team
                """,
            },
            'feedback': {
                'subject': u'🤔 %(sender_name)s has some feedback!',

                'html_body': u"""\
                <p>Feedback from <a href="https://api.reaction.cam/admin/accounts/%(identifier)s/" style="font-weight:600;">%(sender_name)s</a>:</p>
                <blockquote>%(message_html)s</blockquote>
                <p>Client: <code>%(user_agent)s</code></p>
                <p>Email: %(email)s</p>
                """,

                'text_body': u"""\
                Feedback from %(sender_name)s:

                %(message)s

                Account: https://api.reaction.cam/admin/accounts/%(identifier)s/
                Client: %(user_agent)s
                Email: %(email)s
                """,
            },
            'invite': {
                'subject': u'🎉 %(invited_name)s, you’re invited to join me on REACTION.CAM',

                'html_body': u"""\
                <p>Hello <strong style="font-weight:600;">%(invited_name)s</strong>,</p>

                <p>%(inviter_name)s invited you to join
                <a href="https://www.reaction.cam/">REACTION.CAM</a>.</p>

                <p style="text-align:center;">
                    <a class="button" href="https://www.reaction.cam/">Get Started</a>
                </p>

                <p>- REACTION.CAM team</p>

                <p style="font-size:10px;text-align:center;">Open
                <a href="https://www.reaction.cam/">https://www.reaction.cam/</a>
                if the button above won't work.</p>
                """,

                'text_body': u"""\
                Hello %(invited_name)s,

                You've been invited by %(inviter_name)s to join REACTION.CAM. Accept invitation:

                https://www.reaction.cam/
                (open on your iPhone)

                - REACTION.CAM team
                """,
            },
            'login': {
                'subject': u'%(code)s is your REACTION.CAM sign in code',

                'html_body': u"""\
                <p>You requested a code to sign into REACTION.CAM, just enter the one below in the
                app and you're good to go.</p>
                <p><span class="code">%(code)s</span></p>
                <p>If you, or someone on your behalf didn't request a verification code, please disregard this email.</p>
                <p>- REACTION.CAM team</p>
                """,

                'text_body': u"""\
                You requested a code to sign into REACTION.CAM, just enter the one below in the app
                and you're good to go!

                Code: %(code)s

                If you, or someone on your behalf didn't request a verification code, please disregard this email.

                - REACTION.CAM team
                """,
            },
            'unplayed_chunk': {
                'subject': u'%(destination_sentence)s on REACTION.CAM',

                'html_body': u"""\
                <table>
                    <tr>
                        <td valign="top"><a href="https://www.reaction.cam/"><img src="%(thumb)s" style="border-radius:4px;" width="80" height="80"></a></td>
                        <td style="padding:0 0 20px 6px;">
                            <b>%(sender_name)s sent a video at %(time)s.</b><br><br>
                            <a href="https://www.reaction.cam/">Get REACTION.CAM to see it »</a>
                        </td>
                    </tr>
                </table>
                """,

                'text_body': u"""\
                %(sender_name)s sent a video at %(time)s.

                Get REACTION.CAM to watch the video: https://www.reaction.cam/
                """
            },
            'unplayed_chunk_transcript': {
                'subject': u'%(destination_sentence)s on REACTION.CAM',

                'html_body': u"""\
                <table>
                    <tr>
                        <td valign="top"><a href="https://www.reaction.cam/"><img src="%(thumb)s" style="border-radius:4px;" width="80" height="80"></a></td>
                        <td style="padding:0 0 20px 6px;">
                            <b style="font-size:18px;">%(sender_name)s at %(time)s:</b><br>
                            %(transcript)s<br><br>
                            <a href="https://www.reaction.cam/">Get REACTION.CAM to see it »</a>
                        </td>
                    </tr>
                </table>
                """,

                'text_body': u"""\
                %(sender_name)s at %(time)s:
                %(transcript)s

                Get REACTION.CAM to watch the video: https://www.reaction.cam/
                """
            },
        },
    },
}


def schedule_welcome_emails(account, email):
    if account.scheduled_welcome_emails:
        logging.debug('Skipping scheduling welcome emails (already done)')
        return
    account.scheduled_welcome_emails = True
    account.put()
    logging.debug('Scheduling fika.welcome_1 for %s right now', email)
    params = dict(to=email, to_name=account.display_name)
    deferred.defer(send_email, 'fika', 'welcome_1', **params)
    # Establish reasonable mail time for this user.
    eta = datetime.utcnow().replace(tzinfo=pytz.utc)
    tz = account.location_info.timezone if account.location_info else None
    if tz:
        eta = eta.astimezone(pytz.timezone(tz))
    # Send early morning.
    eta = eta.replace(hour=7, minute=0)
    # Schedule all the future emails (first value is how many days apart).
    eta = _add_weekdays(eta, 1)
    _schedule_email(eta, 'fika', 'welcome_2', **params)
    eta = _add_weekdays(eta, 2)
    _schedule_email(eta, 'fika', 'welcome_3', **params)
    eta = _add_weekdays(eta, 2)
    _schedule_email(eta, 'fika', 'welcome_4', **params)
    eta = _add_weekdays(eta, 2)
    _schedule_email(eta, 'fika', 'welcome_5', **params)
    eta = _add_weekdays(eta, 2)
    _schedule_email(eta, 'fika', 'welcome_6', **params)
    eta = _add_weekdays(eta, 2)
    _schedule_email(eta, 'fika', 'welcome_7', **params)


def send_email(app, template_name, to=None, to_name=None, sender=None, sender_name=None,
               cc_sender=True, subject=None, attachments=None, **kwargs):
    app_settings = EMAIL_APP_SETTINGS[app]
    template = app_settings['templates'][template_name]
    if sender:
        sender = mail.Email(sender, sender_name)
    from_ = mail.Email(app_settings['from_email'],
                       sender_name or app_settings['default_from_name'])
    to = mail.Email(to, to_name)
    # Forward name parameters to the template.
    kwargs['sender_name'] = sender_name or (sender.email if sender else 'anonymous')
    kwargs['to_name'] = to_name
    # Create the text and HTML bodies of the email.
    text = textwrap.dedent(template['text_body']) % kwargs
    html = app_settings['html'] % {
        'body': textwrap.dedent(template['html_body']) % kwargs,
    }
    # Construct the email and send it.
    message = mail.Mail()
    message.add_category(mail.Category(template_name))
    p = mail.Personalization()
    p.add_to(to)
    if sender:
        if cc_sender:
            p.add_cc(sender)
        message.reply_to = sender
    message.add_personalization(p)
    message.from_email = from_
    message.subject = subject or (template['subject'] % kwargs)
    message.add_content(mail.Content('text/plain', text))
    message.add_content(mail.Content('text/html', html))
    if attachments:
        # Attachments should be a list of tuples of (filename, data).
        for filename, data in attachments:
            a = mail.Attachment()
            mimetype, _ = mimetypes.guess_type(filename)
            a.filename = filename
            a.type = mimetype
            a.content = base64.b64encode(data)
            message.add_attachment(a)
    try:
        _sendgrid_api.client.mail.send.post(request_body=message.get())
    except Exception as e:
        logging.error('Failed to send email from %s to %s', from_.email, to.email)
        logging.error('Sendgrid error: %r', e)
        logging.debug(json.dumps(message.get()))
        raise errors.ExternalError('Could not send email')
    logging.debug('Sent email from %s to %s', from_.email, to.email)


# TODO: Remove this once no more deferred tasks reference it.
def send_fika_email(*args, **kwargs):
    send_email('fika', *args, **kwargs)


def _add_weekdays(dt, days):
    dt += timedelta(days=days)
    if dt.weekday() in (5, 6):
        # Move to Monday (Sat == 5, Sun == 6).
        dt += timedelta(days=7 - dt.weekday())
    return dt


def _schedule_email(eta, app, template, to, **kwargs):
    logging.debug('Scheduling %s.%s for %s at %s', app, template, to,
                  eta.strftime('%Y-%m-%d %H:%M %Z%z'))
    deferred.defer(send_email, template,
                   to=to, _eta=eta.astimezone(pytz.utc),
                   **kwargs)


# TODO - find a better place for this
LANGUAGE_PREFIXES = dict()
for lang, countries in LANGUAGE_COUNTRIES.iteritems():
    for country in countries:
        LANGUAGE_PREFIXES[lang] = tuple(LANGUAGE_PREFIXES.get(lang, tuple()) +\
                COUNTRY_PREFIXES.get(country, tuple()))


STRING_TEMPLATES = dict()
for filename in os.listdir('strings'):
    if fnmatch.fnmatch(filename, '*.json'):
        lang = filename.split('.')[0]
        with open('strings/%s' % filename) as f:
            STRING_TEMPLATES[lang] = json.load(f)


def _get_country():
    if flask.has_request_context():
        try:
            # TODO: Restrict this header to only our backend.
            location = flask.request.headers[config.FORWARDED_LOCATION_HEADER]
            ip, country, region, city = location.split(',')
        except:
            country = flask.request.headers.get('X-Appengine-Country', 'ZZ')
        return country.upper()
    return 'ZZ'


def get_language(receiver=None):
    # First try to determine language based on receiver, if it fails, fallback to IP
    if receiver:
        for lang, prefixes in LANGUAGE_PREFIXES.iteritems():
            if receiver.startswith(prefixes):
                return lang
    country = _get_country()
    for lang, countries in LANGUAGE_COUNTRIES.iteritems():
        if country in countries:
            return lang
    return None


def get_roger_greeting():
    country = _get_country()
    for payload, duration, countries in COUNTRY_GREETING_LIST:
        if country in countries:
            return payload, duration
    return None, None


def get_string(string_id, args={}, receiver=None):
    lang = get_language(receiver)
    try:
        return STRING_TEMPLATES.get(lang).get(string_id) % args
    except (AttributeError, TypeError):
        return STRING_TEMPLATES.get('en-US').get(string_id) % args
