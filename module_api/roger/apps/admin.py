# -*- coding: utf-8 -*-

import cgi
import collections
from datetime import date, datetime, timedelta
import itertools
import json
import logging
import re
import unicodedata
import urllib
import urlparse

from google.appengine.api import memcache, search, taskqueue
from google.appengine.datastore import datastore_query
from google.appengine.ext import ndb

from flask import Flask, render_template, redirect, request
import pytz

from roger import accounts, bots, config, files, localize, location
from roger import models, notifs, slack_api, streams, strings, threads
from roger.apps import utils
from roger_common import bigquery_api, convert, errors, flask_extras, identifiers, random


def add_cors_headers(response):
    response.headers['Access-Control-Allow-Origin'] = 'https://api.reaction.cam'
    if request.method == 'OPTIONS':
        response.headers['Access-Control-Allow-Methods'] = 'DELETE, GET, POST, PUT'
        headers = request.headers.get('Access-Control-Request-Headers')
        if headers:
            response.headers['Access-Control-Allow-Headers'] = headers
    return response


app = Flask(__name__)
app.after_request(add_cors_headers)


if config.PRODUCTION:
    bigquery_client = bigquery_api.BigQueryClient.for_appengine(
        project_id=config.BIGQUERY_PROJECT,
        dataset_id=config.BIGQUERY_DATASET,
        )
else:
    bigquery_client = None


##########################################################################################
# CONSTANTS ##############################################################################
##########################################################################################


AUTO_SCRIPT = u"""
<script>
let ticking = true;
const go = document.querySelector('.go');
if (go) {
    const text = go.textContent;
    let sec = %d;
    function tick() {
        go.textContent = `${text} (${ticking ? sec : '–'})`;
        if (!ticking) return;
        if (!sec) location.href = go.href; else setTimeout(tick, 100);
        sec--;
    }
    tick();
    go.addEventListener('click', () => { ticking = false; });
}
const pause = document.querySelector('.pause');
if (pause) {
    pause.addEventListener('click', () => {
        ticking = false;
        pause.textContent = 'Paused';
    });
}
</script>
"""


DEFAULT_IDENTIFIERS = [
    'blixt',
    'producthunt',
    'ricardo',
    'roger',
    'shreyas',
    'zq',
]


DEVICE_NAMES = {
    'iPad1,1': 'iPad',
    'iPad1,2': 'iPad 3G',
    'iPad2,1': 'iPad 2 (Wi-Fi Only)',
    'iPad2,2': 'iPad 2 (GSM)',
    'iPad2,3': 'iPad 2 (CDMA)',
    'iPad2,4': 'iPad 2 (Wi-Fi Only, Rev A)',
    'iPad2,5': 'iPad mini (Wi-Fi Only)',
    'iPad2,6': 'iPad mini (GSM)',
    'iPad2,7': 'iPad mini (CDMA)',
    'iPad3,1': 'iPad 3rd Gen (Wi-Fi Only)',
    'iPad3,2': 'iPad 3rd Gen (CDMA)',
    'iPad3,3': 'iPad 3rd Gen (GSM)',
    'iPad3,4': 'iPad 4th Gen (Wi-Fi Only)',
    'iPad3,5': 'iPad 4th Gen (GSM/LTE)',
    'iPad3,6': 'iPad 4th Gen (GSM/CDMA/LTE)',
    'iPad4,1': 'iPad Air (Wi-Fi Only)',
    'iPad4,2': 'iPad Air (CDMA/GSM/LTE)',
    'iPad4,3': 'iPad Air (China)',
    'iPad4,4': 'iPad mini 2 (Wi-Fi Only)',
    'iPad4,5': 'iPad mini 2 (CDMA/GSM/LTE)',
    'iPad4,6': 'iPad mini 2 (China)',
    'iPad4,7': 'iPad mini 3 (Wi-Fi Only)',
    'iPad4,8': 'iPad mini 3 (CDMA/GSM/LTE)',
    'iPad4,9': 'iPad mini 3 (China)',
    'iPad5,1': 'iPad mini 4 (Wi-Fi Only)',
    'iPad5,2': 'iPad mini 4 (CDMA/GSM/LTE)',
    'iPad5,3': 'iPad Air 2 (Wi-Fi Only)',
    'iPad5,4': 'iPad Air 2 (CDMA/GSM/LTE)',
    'iPad6,3': 'iPad Pro 9.7-Inch (Wi-Fi Only)',
    'iPad6,4': 'iPad Pro 9.7-Inch (CDMA/GSM/LTE)',
    'iPad6,7': 'iPad Pro 12.9-Inch (Wi-Fi Only)',
    'iPad6,8': 'iPad Pro 12.9-Inch (CDMA/GSM/LTE)',
    'iPad6,11': 'iPad 9.7-Inch 5th Gen (Wi-Fi Only)',
    'iPad6,12': 'iPad 9.7-Inch 5th Gen (CDMA/GSM/LTE)',
    'iPhone1,1': 'iPhone (1st Gen)',
    'iPhone1,2': 'iPhone 3G',
    'iPhone2,1': 'iPhone 3GS',
    'iPhone3,1': 'iPhone 4 (GSM)',
    'iPhone3,2': 'iPhone 4 (GSM, Rev A)',
    'iPhone3,3': 'iPhone 4 (CDMA)',
    'iPhone4,1': 'iPhone 4S',
    'iPhone5,1': 'iPhone 5 (GSM)',
    'iPhone5,2': 'iPhone 5 (CDMA/GSM)',
    'iPhone5,3': 'iPhone 5c (GSM)',
    'iPhone5,4': 'iPhone 5c (Global)',
    'iPhone6,1': 'iPhone 5s (GSM)',
    'iPhone6,2': 'iPhone 5s (Global)',
    'iPhone7,1': 'iPhone 6 Plus',
    'iPhone7,2': 'iPhone 6',
    'iPhone8,1': 'iPhone 6s',
    'iPhone8,2': 'iPhone 6s Plus',
    'iPhone8,3': 'iPhone SE (CDMA/GSM)',
    'iPhone8,4': 'iPhone SE (GSM)',
    'iPhone9,1': 'iPhone 7 (Verizon/Sprint)',
    'iPhone9,2': 'iPhone 7 Plus (Verizon/Sprint)',
    'iPhone9,3': 'iPhone 7 (AT&T/T-Mobile)',
    'iPhone9,4': 'iPhone 7 Plus (AT&T/T-Mobile)',
    'iPhone10,1': 'iPhone 8',
    'iPhone10,2': 'iPhone 8 Plus',
    'iPhone10,3': 'iPhone X',
    'iPhone10,4': 'iPhone 8',
    'iPhone10,5': 'iPhone 8 Plus',
    'iPhone10,6': 'iPhone X',
    'iPod1,1': 'iPod touch (1st Gen)',
    'iPod2,1': 'iPod touch (2nd Gen)',
    'iPod3,1': 'iPod touch (3rd Gen)',
    'iPod4,1': 'iPod touch (4th Gen)',
    'iPod5,1': 'iPod touch (5th Gen)',
    'iPod7,1': 'iPod touch (6th Gen)',
}


QUERY_CONTENT_TRENDING = """
SELECT
  related_id AS content_id,
  48 - (INTEGER(TIMESTAMP_TO_SEC(CURRENT_TIMESTAMP())/3600) - INTEGER(TIMESTAMP_TO_SEC(timestamp)/3600)) AS h,
  COUNT(*) AS reactions
FROM
  roger_reporting.content_v2
WHERE
  related_id IS NOT NULL
  AND timestamp >= SEC_TO_TIMESTAMP(INTEGER(TIMESTAMP_TO_SEC(CURRENT_TIMESTAMP())/3600-48)*3600)
GROUP BY
  1,
  2
ORDER BY
  1,
  2
"""


QUERY_DASHBOARD_CONVOS = """
SELECT
  SUM(IF(status = 'sent' AND NOT contains_alexa AND num_participants >= 3, 1, 0)) AS group_chunks,
  SUM(IF(status = 'sent' AND contains_alexa, 1, 0)) AS alexa_chunks,
  SUM(IF(status = 'sent' AND NOT contains_alexa AND num_participants < 3, 1, 0)) AS other_chunks,
  INTEGER(SUM(duration)) AS total_seconds,
  COUNT(DISTINCT stream_id) AS unique_conversations,
  COUNT(DISTINCT account_id) AS unique_people,
  COUNT(DISTINCT participant_ids) AS potential_reach
FROM (
  SELECT
    account_id,
    stream_id,
    participant_ids,
    num_participants,
    SOME(participant_ids = 61840001) WITHIN RECORD AS contains_alexa,
    status,
    duration
  FROM
    roger_reporting.stream_v3
  WHERE
    status IN ('played', 'sent')
    AND timestamp > DATE_ADD(CURRENT_TIMESTAMP(), -1, 'DAY')
)
"""


QUERY_DASHBOARD_USERS = """
SELECT key, users FROM [roger-api:ricardo_test.dashboard_users_new_a0]
"""


QUERY_RELEASE_LINK_ACTIVITY = u"""
SELECT
  creator_id,
  content_id,
  activity_type,
  COUNT(*) AS count,
  COUNT(DISTINCT identifier) AS unique_count
FROM
  roger_reporting.content_activity_v1
WHERE
  timestamp >= TIMESTAMP("%s")
  AND timestamp < TIMESTAMP("%s")
GROUP BY
  1,
  2,
  3
ORDER BY
  1,
  2,
  3
LIMIT
  %d
"""


##########################################################################################
# UTILITY CLASSES/FUNCTIONS ##############################################################
##########################################################################################


class Pager(object):
    def __init__(self):
        self.auto_page = False
        self.auto_page_delay = 5
        self.cursor = datastore_query.Cursor(urlsafe=request.args.get('cursor'))
        self.logs = []
        self.next_cursor = None
        try:
            self.page_size = int(request.args['page_size'])
            assert self.page_size > 0
        except:
            self.page_size = 100
        self.processed = 0
        self.record_kind = None
        self.records = []

    @ndb.tasklet
    def add_task_async(self, task, **kwargs):
        yield task.add_async(**kwargs)

    def get_next_html(self, **kwargs):
        if self.next_cursor:
            template = u'Processed {count} {kind}(s) • <a class="go" href="{next_path}">Next</a> • <a class="pause" href="#">Pause</a>'
        else:
            template = u'Processed {count} {kind}(s) • Done!'
        return self._format(template, **kwargs)

    def get_retry_html(self, **kwargs):
        template = u'Processed {count} {kind}(s) • <a class="go" href="{path}">Retry</a> • <a class="pause" href="#">Pause</a>'
        return self._format(template, **kwargs)

    def get_job_list_html(self):
        jobs = []
        for attr, value in self.__class__.__dict__.iteritems():
            if not callable(value):
                continue
            jobs.append('<li><a href="?job={attr}">{attr}</a></li>'.format(attr=attr))
        return 'Available jobs: <ul>%s</ul>' % (''.join(jobs),)

    def log(self, string, *args):
        try:
            line = string % args
        except:
            line = string
        logging.debug(line)
        self.logs.append(line)

    def processed_record(self):
        self.processed += 1

    def run(self, query, **kwargs):
        r, next_cursor, more = query.fetch_page(self.page_size, start_cursor=self.cursor, **kwargs)
        self.next_cursor = next_cursor if more else None
        self.record_kind = query.kind
        self.records = r
        return r

    def _format(self, template, **kwargs):
        qs = dict(urlparse.parse_qsl(request.query_string))
        qs['cursor'] = self.cursor.urlsafe()
        kwargs.setdefault('path', '%s?%s' % (request.path, urllib.urlencode(qs)))
        if self.next_cursor:
            qs['cursor'] = self.next_cursor.urlsafe()
            kwargs.setdefault('next_path', '%s?%s' % (request.path, urllib.urlencode(qs)))
        if self.processed < len(self.records):
            kwargs.setdefault('count', '%d/%d' % (self.processed, len(self.records)))
        else:
            kwargs.setdefault('count', str(self.processed))
        kwargs.setdefault('kind', self.record_kind or 'record')
        lines = [template.format(**kwargs)]
        if self.logs:
            lines.append('--')
            lines.extend(cgi.escape(line) for line in self.logs)
        html = u'<pre>{}</pre>'.format(u'\n'.join(lines))
        if self.auto_page:
            html += AUTO_SCRIPT % (int(self.auto_page_delay * 10),)
        return html


class PagingJobRunner(Pager):
    def boost_reviewed_accounts(self):
        self.auto_page = True
        self.page_size = 100
        q = models.Content.query()
        q = q.filter(models.Content.tags == 'reaction')
        q = q.order(-models.Content.created)
        content_to_put = []
        content_list = list(self.run(q))
        accounts = ndb.get_multi(set(c.creator for c in content_list))
        lookup = {a.key: a for a in accounts}
        for content in content_list:
            creator = lookup[content.creator]
            if creator.quality >= 4:
                bonus = 50000
            elif creator.quality == 3:
                bonus = 30000
            elif creator.quality == 2:
                bonus = 5000
            else:
                continue
            if content.sort_bonus >= bonus:
                # Don't give twice.
                continue
            content.add_sort_index_bonus(bonus)
            content_to_put.append(content)
            self.processed_record()
        ndb.put_multi(content_to_put)

    def fix_slugs(self):
        self.auto_page = True
        self.page_size = 1000
        q = models.Content.query()
        q = q.filter(models.Content.tags == 'original')
        q = q.order(-models.Content.created)
        content_to_put = []
        for content in self.run(q):
            if content.slug:
                continue
            title = '%s %s' % (content.title, random.base62(10))
            content.slug = models.Content.make_slug(title)
            if not content.slug:
                continue
            content_to_put.append(content)
            self.processed_record()
        ndb.put_multi(content_to_put)

    def list_related_content(self):
        self.page_size = 1000
        q = models.Content.query()
        q = q.filter(models.Content.related_to == ndb.Key('Content', 5176847147991040))
        q = q.filter(models.Content.tags == 'reaction')
        content_list = self.run(q)
        content_list.sort(key=lambda c: c.votes_real)
        for content in content_list:
            self.processed_record()
            self.log(content.video_url)

    def lower_case_tags(self):
        self.page_size = 1000
        q = models.Content.query()
        q = q.order(-models.Content.created)
        content_to_put = []
        for content in self.run(q):
            tags = [t.lower() for t in content.tags]
            if tags != content.tags:
                self.processed_record()
                content.tags = tags
                content_to_put.append(content)
        self.log('Oldest content: %s', content.created)
        ndb.put_multi(content_to_put)

    def publish_content_of_rated_accounts(self):
        self.auto_page = True
        self.page_size = 100
        q = models.Account.query()
        q = q.order(-models.Account.created)
        content_to_put = []
        for account in self.run(q):
            if not account.publish:
                continue
            qq = models.Content.query()
            qq = qq.filter(models.Content.creator == account.key)
            qq = qq.filter(models.Content.tags == 'reaction')
            qq = qq.order(-models.Content.created)
            content_list = qq.fetch(5)
            for content in content_list:
                if 'published' in content.tags:
                    continue
                content.add_tag('published', allow_restricted=True)
                content_to_put.append(content)
            self.processed_record()
        self.log('Oldest account: %s', account.created)
        ndb.put_multi(content_to_put)

    def set_content_sort_index(self):
        self.auto_page = True
        self.page_size = 1000
        q = models.Content.query()
        content_to_put = []
        for content in self.run(q):
            if content.sort_index:
                continue
            delta = content.created - datetime(2017, 5, 1)
            content.sort_index = int(delta.total_seconds())
            content_to_put.append(content)
            self.processed_record()
        ndb.put_multi(content_to_put)

    def update_account_content_count(self):
        self.auto_page = True
        self.page_size = 100
        q = models.Account.query(models.Account.created > datetime(2017, 4, 22))
        accounts_to_put = []
        for account in self.run(q):
            if not account.last_active_client or 'ReactionCam' not in account.last_active_client:
                continue
            qq = models.Content.query()
            qq = qq.filter(models.Content.creator == account.key)
            qq = qq.filter(models.Content.tags == 'reaction')
            content_count = qq.count(keys_only=True)
            if content_count == account.content_count:
                continue
            account.content_count = content_count
            accounts_to_put.append(account)
            self.processed_record()
        ndb.put_multi(accounts_to_put)

    def update_content_related_count(self):
        self.auto_page = True
        self.page_size = 100
        q = models.Content.query()
        content_to_put = []
        for content in self.run(q):
            qq = models.Content.query()
            qq = qq.filter(models.Content.related_to == content.key)
            qq = qq.filter(models.Content.tags == 'reaction')
            related_count = qq.count(keys_only=True)
            if related_count == content.related_count:
                continue
            content.related_count = related_count
            content_to_put.append(content)
            self.processed_record()
        ndb.put_multi(content_to_put)

    def update_old_youtube_accounts(self):
        self.auto_page = True
        self.page_size = 100
        delay = 1
        futures = []
        q = models.Identity.query()
        q = q.filter(models.Identity.key > ndb.Key('Identity', 'youtube:'))
        q = q.filter(models.Identity.key < ndb.Key('Identity', 'youtube;'))
        accounts = ndb.get_multi([i.account for i in self.run(q) if i.account])
        for account in accounts:
            if account.youtube_channel_id and account.youtube_subs_updated:
                time_since_update = datetime.utcnow() - account.youtube_subs_updated
                if time_since_update < timedelta(days=2):
                    continue
            task = taskqueue.Task(url='/_ah/jobs/update_youtube_channel',
                                  countdown=delay,
                                  params={'account_id': account.key.id()})
            futures.append(self.add_task_async(task, queue_name=config.INTERNAL_QUEUE))
            self.processed_record()
            delay += 1
        self.auto_page_delay = delay
        ndb.Future.wait_all(futures)

    def update_youtube_views_on_accounts(self):
        self.auto_page = True
        self.page_size = 100
        delay = 1
        futures = []
        q = models.Identity.query()
        q = q.filter(models.Identity.key > ndb.Key('Identity', 'youtube:'))
        q = q.filter(models.Identity.key < ndb.Key('Identity', 'youtube;'))
        accounts = ndb.get_multi([i.account for i in self.run(q) if i.account])
        for account in accounts:
            if not account.content_count:
                # Assume there's nothing to update.
                continue
            if account.youtube_reaction_views_updated:
                time_since_update = datetime.utcnow() - account.youtube_reaction_views_updated
                if time_since_update < timedelta(days=2):
                    continue
            task = taskqueue.Task(url='/_ah/jobs/update_youtube_views_batched',
                                  countdown=delay,
                                  params={'creator_id': account.key.id()})
            futures.append(self.add_task_async(task, queue_name=config.INTERNAL_QUEUE))
            self.processed_record()
            delay += 1
        self.auto_page_delay = delay
        ndb.Future.wait_all(futures)

    def update_thumbnails_reaction(self):
        self.auto_page = True
        self.auto_page_delay = 10
        self.page_size = 10
        futures = []
        q = models.Content.query(models.Content.tags == 'reaction')
        q = q.order(-models.Content.created)
        content = None
        for content in self.run(q):
            if not content.thumb_url and content.video_url:
                task = taskqueue.Task(url='/_ah/jobs/generate_thumbnail',
                                      params={'content_id': content.key.id()})
                futures.append(self.add_task_async(task, queue_name=config.INTERNAL_QUEUE))
                self.processed_record()
        self.log('Oldest content: %s', content.created)
        ndb.Future.wait_all(futures)

    def update_thumbnails_youtube(self):
        self.auto_page = True
        self.page_size = 500
        context = ndb.get_context()
        @ndb.tasklet
        def update_one(content):
            new_thumb_url = content.thumb_url.replace('/hqdefault.jpg', '/maxresdefault.jpg')
            result = yield context.urlfetch(new_thumb_url, method='HEAD')
            if result.status_code != 200:
                return
            content.thumb_url = new_thumb_url
            yield content.put_async()
        futures = []
        q = models.Content.query(models.Content.tags == 'original')
        q = q.order(-models.Content.created)
        content = None
        for content in self.run(q):
            if content.thumb_url and '/hqdefault.jpg' in content.thumb_url:
                futures.append(update_one(content))
                self.processed_record()
        self.log('Oldest content: %s', content.created)
        ndb.Future.wait_all(futures)


def form_to_dict(mapper=lambda k, v: v):
    values = {'properties': {}}
    for key, value in request.form.iteritems():
        if key == 'properties':
            p = json.loads(value)
            if not isinstance(p, dict):
                raise ValueError('"properties" must be a JSON object')
            for k, v in p.iteritems():
                values['properties'][k] = mapper('properties.' + k, v)
        elif key.startswith('properties.'):
            values['properties'][key[11:]] = mapper(key, value)
        else:
            values[key] = mapper(key, value)
    for key, value in upload_files():
        if key == 'properties':
            raise ValueError('Cannot set "properties" to file')
        elif key.startswith('properties.'):
            values['properties'][key[11:]] = mapper(key, value)
        else:
            values[key] = mapper(key, value)
    return values


@ndb.tasklet
def get_auth_and_account(auth):
    account = yield auth.key.parent().get_async()
    raise ndb.Return((auth, account))


def get_default_users():
    users = []
    for identifier in DEFAULT_IDENTIFIERS:
        try:
            users.append(accounts.get_handler(identifier))
        except:
            pass
    return users


def upload_files(multi=False):
    for key, field in request.files.iteritems(multi=multi):
        if not field.filename:
            continue
        path = files.upload(field.filename, field.stream, persist=True)
        yield key, files.storage_url(path)


##########################################################################################
# TEMPLATE FILTERS #######################################################################
##########################################################################################


@app.template_filter('emoji')
def emoji(value):
    if isinstance(value, models.Account):
        if value.status == 'unclaimed':
            return u'🤯'
        elif value.status == 'banned':
            return u'🚷'
        elif value.status == 'deleted':
            return u'🗑'
        elif not value.quality_has_been_set:
            if value.publish_ is None:
                return u'❓'
            elif value.publish_:
                return u'📢'
            else:
                return u'🔒'
        elif value.quality == 0:
            return u'1️⃣'
        elif value.quality == 1:
            return u'2️⃣'
        elif value.quality == 2:
            return u'3️⃣'
        elif value.quality == 3:
            return u'4️⃣'
        elif value.quality >= 4:
            return u'🤩'
    return u'❓'


@app.template_filter('capped')
def capped(text, max_length=25):
    text = text.strip()
    if len(text) > max_length:
        text = text[:max_length-3].strip() + u'…'
    return text


@app.template_filter('datetimewithms')
def datetimewithms(timestamp):
    return timestamp.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]


@app.template_filter('deltafromnow')
def deltafromnow(timestamp):
    if isinstance(timestamp, datetime):
        return datetime.utcnow() - timestamp
    elif isinstance(timestamp, date):
        return date.today() - timestamp
    raise TypeError()


@app.template_filter('freshness')
def freshness(timestamp):
    if isinstance(timestamp, datetime):
        d = datetime.utcnow() - timestamp
    elif isinstance(timestamp, date):
        d = date.today() - timestamp
    else:
        raise TypeError()
    if d < timedelta(hours=1):
        return 'hour'
    elif d < timedelta(days=1):
        return 'day'
    elif d < timedelta(weeks=1):
        return 'week'
    elif d < timedelta(weeks=2):
        return 'fortnight'
    elif d < timedelta(days=365):
        return 'year'
    return 'old'


@app.template_filter('link_identifier')
def link_identifier(identifier):
    try:
        service, team, resource = identifiers.parse_service(identifier)
    except:
        return identifier
    if team:
        path = '/admin/services/%s/teams/%s/' % (service, team)
        return '<a href="%s">%s:%s</a>/%s' % (path, service, team, resource)
    else:
        path = '/admin/services/%s/' % (service,)
        return '<a href="%s">%s</a>:%s' % (path, service, resource)


@app.template_filter('ms')
def ms(timestamp):
    return convert.unix_timestamp_ms(timestamp)


@app.template_filter('pretty')
def pretty(value, timedelta_date_only=False):
    if isinstance(value, (dict, list)):
        encoder = convert.JSONEncoder({}, sort_keys=True, indent=4, separators=(',', ': '))
        return encoder.encode(value)
    elif isinstance(value, (float, int, long)):
        return '{:,}'.format(value)
    elif isinstance(value, timedelta):
        p = []
        n = int(value.total_seconds())
        if timedelta_date_only:
            n //= 86400
            n, rem = divmod(n, 7)
            p.append(str(rem) + 'd')
        else:
            n, rem = divmod(n, 60)
            p.append(str(rem) + 's')
            if n:
                n, rem = divmod(n, 60)
                p.append(str(rem) + 'm')
            if n:
                n, rem = divmod(n, 24)
                p.append(str(rem) + 'h')
            if n:
                n, rem = divmod(n, 7)
                p.append(str(rem) + 'd')
        if n:
            p.append(str(n) + 'w')
        return ' '.join(reversed(p[-3:]))
    return '{}'.format(value)


@app.template_filter('pretty_json')
def pretty_json(value):
    encoder = convert.JSONEncoder({}, sort_keys=True, indent=4, separators=(',', ': '))
    return encoder.encode(value)


@app.template_filter('recent')
def recent(then, threshold=3600):
    if not then:
        return False
    return (datetime.utcnow() - then).total_seconds() < threshold


@app.template_filter('urlencode')
def urlencode(value):
    if isinstance(value, unicode):
        value = value.encode('utf-8')
    return urllib.quote_plus(value)


@app.template_filter('years_ago')
def years_ago(then):
    now = datetime.utcnow()
    return now.year - then.year - ((now.month, now.day) < (then.month, then.day))


##########################################################################################
# ADMIN PAGES ############################################################################
##########################################################################################


@app.route('/admin/', methods=['GET'])
def get_home():
    return render_template('admin_home.html')


@app.route('/admin/accounts/', methods=['GET', 'POST'])
def get_accounts():
    display_name = request.form.get('display-name')
    if display_name:
        handler = accounts.create(display_name=display_name,
                                  status=request.form.get('status'))
        return redirect('/admin/accounts/{}/'.format(handler.account_id))
    cursor = datastore_query.Cursor(urlsafe=request.args.get('cursor'))
    sort = request.args.get('sort') or 'recent'
    q = models.Account.query()
    if sort == 'quality':
        q = q.order(-models.Account.quality_)
        q = q.order(-models.Account.last_active)
    elif sort == 'recent':
        q = q.order(-models.Account.created)
    else:
        return 'Invalid sort.', 400
    account_list, next_curs, more = q.fetch_page(100, start_cursor=cursor)
    handler_list = []
    for account in account_list:
        handler_list.append(accounts.get_handler(account))
    next_cursor = None
    if more and next_curs:
        next_cursor = next_curs.urlsafe()
    return render_template(
            'admin_accounts.html',
            handlers=handler_list,
            valid_statuses=[s for sub in config.VALID_STATUS_TRANSITIONS for s in sub],
            cursor=next_cursor,
            sort=sort)


@app.route('/admin/accounts/top-creators', methods=['GET', 'POST'])
def get_accounts_top_creators():
    account_keys = [ndb.Key('Account', aid) for aid in config.TOP_CREATOR_IDS]
    if flask_extras.get_flag('recalculate'):
        accounts = utils.recalculate_content_reaction_counts(account_keys)
    else:
        accounts = ndb.get_multi(account_keys)
    for key, account in zip(account_keys, accounts):
        if not account:
            logging.error('Failed to load account %d', key.id())
    return render_template('admin_top_creators.html', accounts=accounts)


@app.route('/admin/accounts/top-creators/index.json', methods=['GET'])
def get_accounts_top_creators_index_json():
    def clean_name(name):
        name = re.sub(r'[\-–—•]+', '-', name)
        name = re.sub(r'\s+[\-_|/]+\s+', ' - ', name)
        name = re.sub(r'\([^)]*\)|\[[^\]]*\]|\{[^}]*\}', '', name)
        name = re.sub(r'Video Oficial|Official (Audio|(Music )?Video)', '', name)
        name = re.sub(r'\s+', ' ', name)
        name = re.sub(r'^\s+|\s+$', '', name)
        name = re.sub(r'^-\s*|\s*-$', '', name)
        return name
    def slugify(name):
        nfkd_form = unicodedata.normalize('NFKD', name)
        name = u''.join([c for c in nfkd_form if not unicodedata.combining(c)])
        name = name.lower()
        name = re.sub(r'\W+', ' ', name)
        name = name.strip()
        name = re.sub(r' +', '-', name)
        return urllib.quote(name)
    def content_query(account_key):
        q = models.Content.query()
        q = q.filter(models.Content.creator == account_key)
        q = q.filter(models.Content.tags == 'original')
        q = q.order(-models.Content.created)
        return q
    account_keys = [ndb.Key('Account', aid) for aid in config.TOP_CREATOR_IDS]
    content_list_futures = [content_query(k).fetch_async() for k in account_keys]
    account_futures = ndb.get_multi_async(account_keys)
    search_index = collections.defaultdict(lambda: {'accounts': [], 'videos': []})
    for account_key, account_future, content_list_future in zip(account_keys, account_futures, content_list_futures):
        account = account_future.get_result()
        if not account:
            logging.error('Could not load account %d', account_key.id())
            continue
        account_entry = {
            'image_url': account.image_url,
            'reaction_count': account.content_reaction_count,
            'title': account.display_name,
            'url': 'https://www.reaction.cam/%s' % (account.username,),
        }
        search_index[account.username]['accounts'].append(account_entry)
        content_list = content_list_future.get_result()
        for content in content_list:
            title = clean_name(content.title)
            video_entry = {
                'image_url': content.thumb_url,
                'reaction_count': content.related_count,
                'title': title,
                'url': 'https://www.reaction.cam/v/%s' % (content.slug,),
            }
            slug = slugify(title)
            if not slug:
                slug = slugify(content.title)
            search_index[slug]['videos'].append(video_entry)
            if slug != account.username:
                search_index[slug]['accounts'].append(account_entry)
                search_index[account.username]['videos'].append(video_entry)
    json_data = json.dumps(search_index, sort_keys=True, indent=4, separators=(',', ': '))
    return json_data, 200, {'Content-Type': 'application/json; charset=utf-8'}


@app.route('/admin/accounts/<account_id>/', methods=['GET', 'POST'])
def get_account(account_id):
    data = {
        'valid_statuses': [s for sub in config.VALID_STATUS_TRANSITIONS for s in sub],
    }
    try:
        handler = accounts.get_handler(account_id)
        if account_id != str(handler.account_id):
            logging.debug('Redirect %r -> %d', account_id, handler.account_id)
            return redirect('/admin/accounts/{}/'.format(handler.account_id))
        set_admin = request.args.get('set_admin')
        if set_admin == 'yes_i_am_sure':
            handler.account.admin = True
            handler.account.put()
        elif set_admin in ('false', 'no', 'off'):
            handler.account.admin = False
            handler.account.put()
        streak = request.args.get('streak')
        if streak:
            logging.debug('Emitting streak notif (%r)', streak)
            handler.notifs.emit(notifs.ON_STREAK, days=int(streak))
        if request.form.get('rebuild-identifiers'):
            logging.debug('Rebuilding identifiers')
            handler.account.rebuild_identifiers()
        display_name = request.form.get('change-display-name')
        if display_name:
            logging.debug('Changing display name (%r)', display_name)
            handler.set_display_name(display_name)
        password = request.form.get('password')
        if password:
            logging.debug('Changing password (HIDDEN)')
            handler.set_password(password)
        if not handler.account.properties:
            handler.account.properties = {}
        properties = flask_extras.get_json_properties(
            'properties',
            apply_to_dict=handler.account.properties)
        if properties:
            handler.account.put()
        set_premium = flask_extras.get_flag('set-premium')
        if set_premium is not None:
            if set_premium != handler.premium:
                logging.debug('Setting premium (%r)', set_premium)
                handler.account.premium = set_premium
                handler.account.put()
        set_quality = flask_extras.get_parameter('set-quality')
        if set_quality is not None:
            logging.debug('Setting quality (%r)', set_quality)
            handler.account.quality = int(set_quality)
            handler.account.put()
        set_verified = flask_extras.get_flag('set-verified')
        if set_verified is not None:
            if set_verified != handler.verified:
                logging.debug('Setting verified (%r)', set_verified)
                handler.account.verified = set_verified
                handler.account.put()
        set_first_chunk_played = flask_extras.get_flag('set-first-chunk-played')
        if set_first_chunk_played is not None:
            if set_first_chunk_played != handler.first_chunk_played:
                logging.debug('Setting "First Chunk Played" (%r)', set_first_chunk_played)
                handler.account.first_chunk_played = set_first_chunk_played
                handler.account.put()
        set_has_roger = flask_extras.get_flag('set-has-roger')
        if set_has_roger is not None:
            if set_has_roger != handler.has_roger:
                logging.debug('Setting "Has Roger" (%r)', set_has_roger)
                handler.account.has_roger = set_has_roger
                handler.account.put()
        add_100_to_wallet_id = request.form.get('add-100-to-wallet')
        if add_100_to_wallet_id:
            logging.debug('Adding 100 Coins to wallet %r', add_100_to_wallet_id)
            rcam_key = ndb.Key('Account', config.REACTION_CAM_ID)
            wallet_key = ndb.Key('Wallet', add_100_to_wallet_id)
            future = models.Wallet.create_tx_async(
                rcam_key, ndb.Key('Wallet', 'admin_pool'),
                wallet_key, 100, u'100 Coins from admin')
            tx = future.get_result()
            _, _, _, _ = tx().get_result()
        schedule_welcome_emails = request.form.get('schedule-welcome-emails')
        if schedule_welcome_emails:
            if handler.scheduled_welcome_emails:
                raise ValueError('Welcome emails were already scheduled for some other reason')
            logging.debug('Scheduling welcome emails')
            # The value is the email to schedule the emails to.
            localize.schedule_welcome_emails(handler.account, schedule_welcome_emails)
        set_scheduled_welcome_emails = flask_extras.get_flag('set-scheduled-welcome-emails')
        if set_scheduled_welcome_emails is not None:
            if set_scheduled_welcome_emails != handler.scheduled_welcome_emails:
                logging.debug('Setting "Scheduled Welcome Emails" (%r)', set_scheduled_welcome_emails)
                handler.account.scheduled_welcome_emails = set_scheduled_welcome_emails
                handler.account.put()
        remove_identifier = request.form.get('remove-identifier')
        if remove_identifier:
            logging.debug('Removing identifier (%r)', remove_identifier)
            handler.remove_identifier(remove_identifier)
        add_identifier = request.form.get('add-identifier')
        if add_identifier:
            logging.debug('Adding identifier (%r)', add_identifier)
            handler.add_identifier(add_identifier, ignore_reserved=True)
        primary_identifier = request.form.get('set-primary-identifier')
        if primary_identifier:
            logging.debug('Setting primary identifier (%r)', primary_identifier)
            handler.set_primary_identifier(primary_identifier)
        callback_url = request.form.get('set-callback-url')
        if callback_url is not None:
            logging.debug('Setting callback URL (%r)', callback_url)
            try:
                callback_version = int(request.form['set-callback-version'])
                handler.account.callback_url = callback_url or None
                handler.account.callback_version = callback_version if callback_url else None
                handler.account.put()
            except ValueError:
                data['error'] = 'Invalid callback API version'
            except Exception as e:
                data['error'] = e
        status = request.form.get('set-status')
        if status and status != handler.status:
            logging.debug('Setting status (%r)', status)
            handler.change_status(status, status_reason='admin_tool')
        image = request.files.get('image')
        if image:
            logging.debug('Setting image (%r)', image)
            payload = files.upload(image.filename, image.stream, persist=True)
            handler.set_image(payload)
        autoreply_delete = request.form.getlist('autoreply-delete')
        if autoreply_delete:
            logging.debug('Deleting auto reply (%r)', autoreply_delete)
            handler.account.autoreplies = [a for a in handler.account.autoreplies
                                           if a.source not in autoreply_delete]
        autoreply = request.files.getlist('autoreply')
        autoreply_duration = request.form.getlist('autoreply-duration')
        if autoreply:
            assert len(autoreply) == len(autoreply_duration)
            logging.debug('Adding autoreplies (%r, %r)', autoreply, autoreply_duration)
            for f, d in zip(autoreply, autoreply_duration):
                source = files.upload(f.filename, f.stream, persist=True)
                reply = models.Reply(source=source, duration=int(d))
                handler.account.autoreplies.append(reply)
        if autoreply or autoreply_delete:
            handler.account.put()
        greeting = request.files.get('greeting')
        if greeting:
            logging.debug('Setting greeting (%r)', greeting)
            payload = files.upload(greeting.filename, greeting.stream, persist=True)
            duration = int(request.form.get('greeting-duration'))
            handler.set_greeting(payload, duration)
        unblock_identifier = request.form.get('unblock-identifier')
        if unblock_identifier:
            logging.debug('Unblocking %r', unblock_identifier)
            handler.unblock(unblock_identifier)
        latlng = request.form.get('location-latlng')
        if latlng:
            point = ndb.GeoPt(latlng)
            if request.form.get('location-custom'):
                info = location.LocationInfo(
                    location=point,
                    city=request.form.get('location-city'),
                    country=request.form.get('location-country'),
                    timezone=request.form.get('location-timezone'))
            else:
                info = location.LocationInfo.from_point(point)
            logging.debug('Setting location (%r)', info)
            handler.account.location_info = info
            handler.account.put()
        data['handler'] = handler
        data['properties'] = json.dumps(handler.properties, sort_keys=True,
                                        indent=4, separators=(',', ': '))
    except errors.ResourceNotFound as e:
        data['error'] = e
    except Exception as e:
        logging.exception('Error in admin tool')
        data['error'] = e
    return render_template('admin_account.html', **data)


@app.route('/admin/accounts/<identifier>/streams/', methods=['GET', 'POST'])
def get_account_streams(identifier):
    data = {
        'cursor': None,
        'recents': [],
    }
    user = accounts.get_handler(identifier)
    if identifier != str(user.account_id):
        return redirect('/admin/accounts/{}/streams/'.format(user.account_id))
    data['user'] = user
    create_stream_identifiers = flask_extras.get_parameter('create-stream-identifiers')
    if flask_extras.get_flag('create-stream'):
        participant_ids = re.split(r'\s*,\s*', create_stream_identifiers)
        try:
            participants = map(lambda i: accounts.get_account(i), participant_ids)
            stream = user.streams.get_or_create(participants)
            return redirect('/admin/accounts/{}/streams/{}'.format(user.account_id, stream.key.id()))
        except Exception as e:
            logging.exception('Error in admin tool')
            data['error'] = e
    cursor = flask_extras.get_parameter('next')
    action = flask_extras.get_parameter('action')
    selected = map(lambda s: streams.get_by_id(user.account, s),
                   flask_extras.get_parameter_list('selected'))
    if action == 'hide':
        for stream in selected:
            stream.hide()
    elif action == 'leave':
        for stream in selected:
            stream.leave()
    try:
        r, data['cursor'] = user.streams.get_recent(max_results=250, cursor=cursor)
        for stream in r:
            for c in reversed(stream.chunks):
                if c.sender != user.account.key:
                    last_chunk_end = c.end
                    break
            else:
                last_chunk_end = datetime.min
            data['recents'].append(stream)
    except Exception as e:
        logging.exception('Error in admin tool')
        data['error'] = e
    return render_template('admin_account_streams.html', **data)


@app.route('/admin/accounts/<identifier>/streams/<stream_id>', methods=['GET', 'POST'])
def get_account_stream(identifier, stream_id):
    user = accounts.get_handler(identifier)
    if identifier != str(user.account_id):
        return redirect('/admin/accounts/{}/streams/{}'.format(user.account_id, stream_id))
    try:
        update_content_id = flask_extras.get_parameter('update-content-id')
        if update_content_id:
            service_key, team_key, resource = models.Service.parse_identifier(update_content_id)
            assert service_key.id() == 'slack'
            auth = user.get_auth_key(service_key, team_key).get()
            # TODO: Put this API call elsewhere!
            if resource.startswith('C'):
                info = slack_api.get_channel_info(resource, auth.access_token)
                channel = info.get('channel')
            elif resource.startswith('G'):
                info = slack_api.get_group_info(resource, auth.access_token)
                channel = info.get('group')
            else:
                raise errors.InvalidArgument('Unrecognized resource')
            if not info['ok']:
                raise errors.ExternalError('Something went wrong')
            user.streams.join_service_content(update_content_id,
                service_members=channel['members'],
                title='#%s' % (channel['name'],))
        stream = user.streams.get_by_id(int(stream_id), all_chunks=True)
        set_visible = flask_extras.get_flag('set-visible')
        if set_visible is not None:
            stream.show() if set_visible else stream.hide()
        set_title = flask_extras.get_parameter('set-title')
        if set_title is not None:
            stream.set_title(set_title)
    except:
        logging.exception('Failed to get stream')
        user = None
        stream = None
    return render_template('admin_account_stream.html',
                           user=user,
                           stream=stream)


@app.route('/admin/accounts/<identifier>/threads/', methods=['GET', 'POST'])
def get_account_threads(identifier):
    data = {
        'cursor': None,
        'error': None,
        'recents': [],
        'user': None,
    }
    user = accounts.get_handler(identifier)
    if identifier != str(user.account_id):
        return redirect('/admin/accounts/{}/threads/'.format(user.account_id))
    data['user'] = user
    cursor = flask_extras.get_parameter('next')
    action = flask_extras.get_parameter('action')
    selected = flask_extras.get_parameter_list('selected')
    selected = [threads.Handler(user.key).get_by_id_async(i) for i in selected]
    selected = [f.get_result() for f in selected]
    if action == 'hide':
        for thread in selected:
            thread.hide()
    elif action == 'hide-for-all':
        for thread in selected:
            thread.hide_for_all()
    elif action == 'leave':
        for thread in selected:
            thread.leave()
    try:
        r, cursor = user.threads.get_recent_threads(cursor=cursor, limit=250)
        data['cursor'] = cursor.urlsafe() if cursor else None
        for thread in r:
            data['recents'].append(thread)
    except Exception as e:
        logging.exception('Error in admin tool')
        data['error'] = e
    return render_template('admin_account_threads.html', **data)


@app.route('/admin/accounts/<identifier>/threads/<thread_id>', methods=['GET', 'POST'])
def get_account_thread(identifier, thread_id):
    account_key = models.Account.resolve_key(identifier)
    account_id = account_key.id()
    if identifier != str(account_id):
        return redirect('/admin/accounts/{}/threads/{}'.format(account_id, thread_id))
    handler = threads.Handler(account_key)
    thread, messages, cursor = handler.get_recent_messages(thread_id, cursor=None)
    return render_template('admin_account_thread.html',
        cursor=cursor.urlsafe() if cursor else None,
        thread=thread,
        messages=messages)


@app.route('/admin/content/', methods=['GET', 'POST'])
@app.route('/admin/content/<int:content_id>/', methods=['GET', 'POST'])
def get_content(content_id=None):
    content = None
    error = None
    if content_id is not None:
        try:
            content = models.Content.get_by_id(int(content_id), use_cache=False, use_memcache=False)
            if not content:
                raise ValueError('Content does not exist')
        except Exception as e:
            error = e

    if content and flask_extras.get_flag('unset_related'):
        content.related_to = None
        content.put()

    created = request.form.get('created')
    if created:
        created = datetime.strptime(created, '%Y-%m-%d %H:%M:%S.%f')
    creator_id = request.form.get('creator-id', str(content.creator.id()) if content and content.creator else '')
    related_to_id = request.form.get('related-to-id', request.args.get('related_to'))
    if not related_to_id:
        related_to_id = str(content.related_to.id()) if content and content.related_to else ''
    video_url = request.form.get('video-url', content.video_url if content else '')
    duration = request.form.get('video-duration', content.duration if content else '')
    properties = dict(content.properties) if content and content.properties else {}
    try:
        flask_extras.get_json_properties('properties', apply_to_dict=properties)
    except Exception as e:
        error = e
    properties_json = json.dumps(properties, sort_keys=True, indent=4, separators=(',', ': '))
    tags = request.form.get('tags', ', '.join(content.tags) if content else '')
    sort_bonus = request.form.get('sort-bonus', str(content.sort_bonus) if content else '')
    sort_bonus_penalty = request.form.get('sort-bonus-penalty', str(content.sort_bonus_penalty) if content else '')
    sort_index = request.form.get('sort-index', str(content.sort_index) if content and content.sort_index is not None else '')
    views = request.form.get('views', str(content.views) if content else '')
    votes = request.form.get('votes', str(content.votes) if content else '')

    title = request.form.get('title', (content.title or '') if content else '')
    slug = request.form.get('slug', content.slug if content else '')
    thumb_url = request.form.get('thumb-url', (content.thumb_url or '') if content else '')
    original_url = request.form.get('original-url', (content.original_url or '') if content else '')
    youtube_id = request.form.get('youtube-id', (content.youtube_id or '') if content else '')
    creator_twitter = request.form.get('creator-twitter', (content.creator_twitter or '') if content else '')
    creator_twitter = ', '.join(map(lambda h: h.strip().lstrip('@'), creator_twitter.split(',')))

    from_list = request.args.get('from_list')
    if not from_list:
        from_list = content.tags[0] if content and content.tags else 'reaction'

    try:
        by = accounts.get_handler(creator_id) if creator_id else None
        if by:
            creator_id = str(by.key.id())
    except Exception as e:
        by = None
        error = e

    def create_or_update(content=None):
        if not creator_id:
            raise ValueError('Missing creator id')
        if content:
            was_featured = 'featured' in content.tags
            content.set_tags(tags, allow_restricted=True, keep_restricted=False)
        else:
            content = models.Content.new(useragent='admin', allow_restricted_tags=True, tags=tags)
            was_featured = False
        if created:
            content.created = created
        content.creator = ndb.Key('Account', int(creator_id))
        content.related_to = ndb.Key('Content', int(related_to_id)) if related_to_id else None
        content.video_url = video_url or None
        content.duration = int(duration or '0')
        content.properties = properties
        if sort_index:
            content.sort_index = int(sort_index)
        content.sort_bonus = int(sort_bonus or '0')
        content.sort_bonus_penalty = int(sort_bonus_penalty or '0')
        if views:
            content.views = int(views)
        if votes:
            content.votes = int(votes)
        content.title = title or None
        content.slug = slug or None
        content.thumb_url = thumb_url or None
        content.original_url = original_url or None
        content.creator_twitter = creator_twitter or None
        if slug:
            existing_content = models.Content.query(models.Content.slug == slug).get()
            if existing_content and existing_content.key != content.key:
                raise ValueError('Slug already taken (%d)' % (existing_content.key.id(),))
        content.set_youtube_id(youtube_id or None)
        content.put()
        if by and 'featured' in content.tags and not was_featured:
            by.notifs.emit(notifs.ON_CONTENT_FEATURED, content=content)
        return content

    if request.method == 'POST':
        try:
            did_exist = bool(content and content.key)
            content = create_or_update(content)
            if not did_exist:
                if from_list:
                    return redirect('/admin/content/%d/?from_list=%s' % (content.key.id(), from_list))
                else:
                    return redirect('/admin/content/%d/' % (content.key.id(),))
        except Exception as e:
            error = e

    from_list_tags = models.Content.parse_tags(from_list, allow_restricted=True, separator='+')

    return render_template('admin_content.html',
        by=by,
        config=config,
        content=content,
        error=error,
        youtube_url=request.args.get('youtube_url'),
        content_id=content_id,
        creator_id=creator_id,
        creator_twitter=creator_twitter,
        related_to_id=related_to_id,
        video_url=video_url,
        duration=duration,
        properties=properties_json,
        tags=tags,
        sort_bonus=sort_bonus,
        sort_bonus_penalty=sort_bonus_penalty,
        sort_index=sort_index,
        views=views,
        votes=votes,
        title=title,
        slug=slug,
        thumb_url=thumb_url,
        original_url=original_url,
        from_list=from_list,
        from_list_tags=from_list_tags)


@app.route('/admin/content/list/<tags>/', methods=['GET', 'POST'])
def get_content_list_tag(tags):
    tags = models.Content.parse_tags(tags, allow_restricted=True, separator='+')
    cursor = datastore_query.Cursor(urlsafe=request.args.get('cursor'))
    q = models.Content.query()
    for tag in tags:
        q = q.filter(models.Content.tags == tag)
    related_to = request.args.get('related_to')
    if related_to:
        related_to = models.Content.get_by_id(int(related_to))
        q = q.filter(models.Content.related_to == related_to.key)
    by = request.args.get('by')
    if by:
        sort = None
        by = models.Account.resolve(by)
        q = q.filter(models.Content.creator == by.key)
        q = q.order(-models.Content.created)
        # Support assigning content.
        assign_content_id = flask_extras.get_parameter('assign-content-id')
        if assign_content_id:
            assign_content = models.Content.get_by_id(int(assign_content_id))
            if assign_content and assign_content.creator != by.key:
                # TODO: More?
                assign_content.creator = by.key
                assign_content.put()
    else:
        sort = request.args.get('sort')
        if not sort:
            sort = 'hot'
        if sort == 'hot':
            q = q.order(-models.Content.sort_index)
        elif sort == 'recent':
            q = q.order(-models.Content.created)
        elif sort == 'top':
            q = q.order(-models.Content.sort_bonus)
        else:
            raise ValueError('unknown sort value')
    content_list, next_curs, more = q.fetch_page(50, start_cursor=cursor)
    account_keys = {c.creator for c in content_list if c.creator}
    content_keys = {c.related_to for c in content_list if c.related_to}
    entities = ndb.get_multi(account_keys | content_keys)
    lookup = {e.key: e for e in entities if e}
    data = []
    for content in content_list:
        result = {
            'content': content,
            'creator': lookup.get(content.creator),
            'related_to': lookup.get(content.related_to),
        }
        data.append(result)
    next_cursor = None
    if more and next_curs:
        next_cursor = next_curs.urlsafe()
    listed_tags = collections.OrderedDict([
        ('featured', 'success'),
        ('reaction', 'primary'),
        ('vlog', 'primary'),
        ('repost', 'warning'),
        ('original', 'info'),
        ('is hot', 'dark'),
        ('is reacted', 'dark'),
        ('is suggestion', 'dark'),
        ('flagged', 'danger'),
    ])
    if by and tags == {'original'} and not related_to:
        cache_key_format = '`user_original_${version}_%s_${sort}_${limit}_None`' % (by.key.id(),)
        cache_key_limit = 20
        cache_key_sort = 'recent'
    elif related_to and not by:
        cache_key_format = '`related_${version}_%s_%s_${sort}_${limit}`' % (related_to.key.id(), '+'.join(sorted(tags)))
        cache_key_limit = 20
        cache_key_sort = 'top'
    elif not by and not related_to:
        if tags == {'is hot', 'original'}:
            cache_key_format = '`original_${version}_${sort}_${limit}_None`'
            cache_key_limit = 10
            cache_key_sort = 'hot'
        else:
            cache_key_format = '`content_${version}_%s_${sort}_${limit}`' % ('+'.join(sorted(tags)),)
            if tags == {'featured'}:
                cache_key_limit = 5
                cache_key_sort = 'recent'
            else:
                cache_key_limit = 50
                cache_key_sort = 'hot'
    else:
        cache_key_format = None
        cache_key_limit = None
        cache_key_sort = None
    return render_template('admin_content_list.html',
        by=by,
        cache_key_format=cache_key_format,
        cache_key_limit=cache_key_limit,
        cache_key_sort=cache_key_sort,
        config=config,
        content_list=data,
        cursor=next_cursor,
        listed_tags=listed_tags,
        related_to=related_to,
        sort=sort,
        tags=tags)


@app.route('/admin/content/review/', methods=['GET'])
def get_content_review():
    if request.host == 'api.rogertalk.com':
        return redirect('https://api.reaction.cam/admin/content/review/')
    cursor = datastore_query.Cursor(urlsafe=request.args.get('cursor'))
    q = models.Content.query()
    q = q.filter(models.Content.tags == 'reaction')
    q = q.order(-models.Content.created)
    content_list, next_cursor, more = q.fetch_page(1000, start_cursor=cursor)
    account_map = {a.key: a for a in ndb.get_multi({c.creator for c in content_list})}
    review_map = collections.OrderedDict()
    for content in content_list:
        creator = account_map[content.creator]
        if creator.key not in review_map:
            review_map[creator.key] = {
                'creator': account_map[creator.key],
                'content': [],
            }
        review_map[creator.key]['content'].append(content)
    return render_template('admin_content_review.html',
        cursor=next_cursor.urlsafe() if more else '',
        review_list=review_map.values())


@app.route('/admin/content/search', methods=['GET'])
def get_content_search():
    if request.host == 'api.rogertalk.com':
        return redirect('https://api.reaction.cam/admin/content/search')
    return render_template('admin_content_search.html')


@app.route('/admin/reactions/', methods=['GET'])
def get_reactions():
    return redirect('/admin/content/list/reaction/?sort=recent')


@app.route('/admin/releases/', methods=['GET'])
def get_releases():
    return render_template('admin_releases.html')


@app.route('/admin/releases/monitor', methods=['GET'])
def get_releases_monitor():
    tz = pytz.timezone('America/New_York')
    midnight = pytz.utc.localize(datetime.utcnow()).astimezone(tz)
    midnight = midnight.replace(hour=0, minute=0, second=0, microsecond=0)
    midnight = midnight.astimezone(pytz.utc)
    report_date = midnight.strftime('%Y%m%d')
    return render_template('admin_releases_monitor.html', report_date=report_date)


@app.route('/admin/requests/', methods=['GET'])
def get_requests():
    # Get active requests.
    # TODO: Paginate all requests?
    q1 = models.ContentRequestPublic.query()
    q1 = q1.order(-models.ContentRequestPublic.sort_index)
    q1 = q1.filter(models.ContentRequestPublic.tags == 'approved')
    q1_future = q1.fetch_async()
    # Get archived requests.
    q2 = models.ContentRequestPublic.query()
    q2 = q2.order(-models.ContentRequestPublic.sort_index)
    q2 = q2.filter(models.ContentRequestPublic.tags == 'archived')
    q2_future = q2.fetch_async()
    # Get pending requests.
    cursor = datastore_query.Cursor(urlsafe=request.args.get('cursor'))
    q3 = models.ContentRequestPublic.query()
    q3 = q3.order(-models.ContentRequestPublic.sort_index)
    q3 = q3.filter(models.ContentRequestPublic.tags == 'pending')
    q3_future = q3.fetch_page_async(100, start_cursor=cursor)
    # Get the results from the two queries.
    # TODO: Get denied requests?
    active_requests = q1_future.get_result()
    archived_requests = q2_future.get_result()
    pending_requests, next_cursor, more = q3_future.get_result()
    # Count active/pending-review entries for active requests.
    CRPE = models.ContentRequestPublicEntry
    future_counts = {}
    for r in active_requests:
        q = CRPE.query(CRPE.request == r.key)
        future_counts[r.key] = (
            q.filter(CRPE.status == 'active').count_async(),
            q.filter(CRPE.status == 'pending-review').count_async())
    # Look up all content, accounts returned in the queries.
    lookup_keys = set()
    for r in itertools.chain(active_requests, archived_requests, pending_requests):
        lookup_keys.update([r.content, r.requested_by, r.wallet])
    lookup_keys.discard(None)
    lookup = {e.key: e for e in ndb.get_multi(lookup_keys)}
    def mapper(r):
        c = lookup[r.content]
        c_props = c.properties or {}
        r_props = r.properties or {}
        result = {
            'content': c,
            'request': r,
            'requested_by': lookup[r.requested_by],
            'subtitle': r_props.get('subtitle') or c_props.get('creator_label'),
            'title': r_props.get('title') or c_props.get('title_short') or c.title,
            'wallet': lookup.get(r.wallet),
        }
        if 'approved' in r.tags:
            f1, f2 = future_counts[r.key]
            result['entries_active'] = f1.get_result()
            result['entries_pending_review'] = f2.get_result()
        else:
            result['entries_active'] = None
            result['entries_pending_review'] = None
        return result
    return render_template('admin_requests.html',
        active_items=map(mapper, active_requests),
        archived_items=map(mapper, archived_requests),
        pending_items=map(mapper, pending_requests),
        next_cursor=next_cursor.urlsafe() if more else None)


@app.route('/admin/requests/new/', methods=['GET', 'POST'])
@app.route('/admin/requests/<int:request_id>/', methods=['GET', 'POST'])
def get_requests_request_id(request_id=None):
    request_needs_put = False
    if request_id:
        req = models.ContentRequestPublic.get_by_id(request_id)
        if not req:
            return 'Invalid request id', 404
    else:
        if request.method != 'POST':
            return render_template('admin_request.html', item=None)
        req = models.ContentRequestPublic()
        request_needs_put = True
    # Update the content reference.
    try:
        content_id = int(flask_extras.get_parameter('content-id'))
        content_key = ndb.Key('Content', content_id)
    except:
        content_id = None
        content_key = None
    if content_key and content_key != req.content:
        req.content = content_key
        request_needs_put = True
    # Update the account reference.
    requested_by_identifier = flask_extras.get_parameter('requested-by')
    if requested_by_identifier:
        requested_by_key = models.Account.resolve_key(requested_by_identifier)
        if requested_by_key != req.requested_by:
            req.requested_by = requested_by_key
            request_needs_put = True
    # Update the sort index.
    sort_index = flask_extras.get_parameter('sort-index')
    if sort_index is not None:
        if sort_index:
            sort_index = int(sort_index)
        else:
            sort_index = models.Content.get_sort_index()
        if sort_index != req.sort_index:
            req.sort_index = sort_index
            request_needs_put = True
    # Update tags.
    tags = flask_extras.get_parameter('tags')
    if tags is not None:
        tags = models.Content.parse_tags(tags, allow_restricted=True)
        if tags != set(req.tags):
            req.tags = sorted(tags)
            request_needs_put = True
    # Update properties.
    if not req.properties:
        req.properties = {}
    properties = flask_extras.get_json_properties(
        'properties',
        apply_to_dict=req.properties)
    if properties:
        request_needs_put = True
    if req.content and req.requested_by:
        content, requested_by = ndb.get_multi([req.content, req.requested_by])
    else:
        content, requested_by = None, None
    if not content:
        return 'Invalid content.', 400
    if not requested_by:
        return 'Invalid account.', 400
    # TODO: Handle more actions.
    action = flask_extras.get_parameter('action')
    if action == 'approve':
        if 'approved' not in req.tags:
            req.set_state('approved')
            request_needs_put = True
    elif action == 'archive':
        if 'archived' not in req.tags:
            req.set_state('archived')
            request_needs_put = True
    elif action == 'close':
        if not req.closed:
            req.closed = True
            request_needs_put = True
    elif action == 'deny':
        if 'denied' not in req.tags:
            req.set_state('denied')
            request_needs_put = True
    elif action == 'reopen':
        if req.closed:
            req.closed = False
            request_needs_put = True
    # Create wallet.
    new_wallet_balance = flask_extras.get_parameter('new-wallet-balance')
    new_wallet_owner_id = flask_extras.get_parameter('new-wallet-owner')
    if new_wallet_balance:
        if req.wallet:
            return 'Request already has a wallet', 400
        initial_balance = int(new_wallet_balance)
        account_key = models.Account.resolve_key(new_wallet_owner_id)
        if not account_key:
            return 'Must specify wallet owner', 400
        # TODO: Change ContentRequestPublic.wallet_owner to be stored to change this.
        if account_key != req.requested_by:
            return 'Wallet owner must match requested_by account', 400
        wallet_id = 'request_%d_reward' % (req.key.id(),)
        wallet = models.Wallet.create_internal(account_key, wallet_id, initial_balance,
                                               'Request reward pool')
        req.wallet = wallet.key
        request_needs_put = True
    # Update the request if anything changed.
    if request_needs_put:
        req.put()
        if not request_id:
            return redirect('/admin/requests/%d/' % (req.key.id(),))
    # Prepare data for template.
    c_props = content.properties or {}
    r_props = req.properties
    item = {
        'content': content,
        'request': req,
        'requested_by': requested_by,
        'subtitle': r_props.get('subtitle') or c_props.get('creator_label'),
        'title': r_props.get('title') or c_props.get('title_short') or content.title,
        'wallet': req.wallet.get() if req.wallet else None,
    }
    showing_extra_entries = flask_extras.get_flag('show_extras')
    # Get the entries to the request.
    CRPE = models.ContentRequestPublicEntry
    q = CRPE.query()
    q = q.filter(CRPE.request == req.key)
    q = q.order(-CRPE.created)
    f_active = q.filter(CRPE.status == 'active').fetch_async()
    f_inactive = q.filter(CRPE.status == 'inactive').fetch_async()
    f_review = q.filter(CRPE.status == 'pending-review').fetch_async()
    if showing_extra_entries:
        f_denied = q.filter(CRPE.status == 'denied').fetch_async()
        f_pending = q.filter(CRPE.status.IN(['pending-upload', 'pending-youtube'])).fetch_async()
        e_denied = f_denied.get_result()
        e_pending = f_pending.get_result()
    else:
        e_denied = []
        e_pending = []
    e_active = f_active.get_result()
    e_inactive = f_inactive.get_result()
    e_review = f_review.get_result()
    lookup_keys = set()
    for e in itertools.chain(e_active, e_denied, e_inactive, e_pending, e_review):
        lookup_keys.update([e.account, e.content])
    lookup_keys.discard(None)
    lookup = {e.key: e for e in ndb.get_multi(lookup_keys)}
    def mapper(e):
        return {
            'account': lookup.get(e.account),
            'content': lookup.get(e.content),
            'entry': e,
        }
    return render_template('admin_request.html',
        active_entries=map(mapper, e_active),
        denied_entries=map(mapper, e_denied),
        inactive_entries=map(mapper, e_inactive),
        pending_entries=map(mapper, e_pending),
        review_entries=map(mapper, e_review),
        showing_extra_entries=showing_extra_entries,
        item=item)


@app.route('/admin/requests/<int:request_id>/<identifier>', methods=['GET', 'POST'])
def get_requests_request_id_identifier(request_id, identifier):
    account = models.Account.resolve(identifier)
    if not account:
        return 'Not found.', 404
    # Entry id always matches account id.
    entry_id = account.key.id()
    request_key = ndb.Key('ContentRequestPublic', request_id)
    entry_key = ndb.Key('ContentRequestPublicEntry', '%d.%d' % (request_id, entry_id))
    request, entry = ndb.get_multi([request_key, entry_key])
    if not request or not entry:
        return 'Not found.', 404
    content_id = flask_extras.get_parameter('content-id')
    if content_id:
        content_id = int(content_id)
        if not entry.content or content_id != entry.content.id():
            return 'Entry content changed since page load.', 400
    request_or_entry_changed = False
    futures = []
    # TODO: Handle more actions.
    action = flask_extras.get_parameter('action')
    if action in ('approve', 'deny'):
        if entry.status != 'pending-review':
            return 'Invalid status.', 400
        entry = models.ContentRequestPublicEntry.review(entry.key, action == 'approve')
        if entry.status == 'active':
            # Immediately update reward on entry.
            task = taskqueue.Task(
                countdown=2,
                url='/_ah/jobs/update_content_request_entry',
                params={
                    'account_id': str(entry.account.id()),
                    'content_id': str(entry.content.id()),
                    'request_id': str(entry.request.id()),
                    'wallet_id': str(request.wallet.id()),
                    'wallet_owner_id': str(request.wallet_owner.id()),
                    'youtube_id': entry.youtube_id,
                },
                retry_options=taskqueue.TaskRetryOptions(task_retry_limit=0))
            futures.append(_add_task_async(task, queue_name=config.INTERNAL_QUEUE))
        request_or_entry_changed = True
    elif action == 'reset':
        entry, _ = models.ContentRequestPublicEntry.update(entry.key, None, reset=True)
        request_or_entry_changed = True
    elif action == 'restore':
        entry = models.ContentRequestPublicEntry.restore(entry.key)
        request_or_entry_changed = True
    else:
        return 'Unknown action.', 400
    if request_or_entry_changed:
        # Let user know something happened.
        hub = notifs.Hub(entry.account)
        futures.append(hub.emit_async(notifs.ON_PUBLIC_REQUEST_UPDATE, request_id=request_id))
    ndb.Future.wait_all(futures)
    return ''


@app.route('/admin/update_content/', methods=['GET'])
@app.route('/admin/update_content/<content_id>/', methods=['GET', 'POST'])
def get_update_content(content_id=None):
    content = models.ExportedContent.get_by_id(content_id) if content_id else None
    if not content:
        return render_template('admin_update_content.html', content=None)
    stream, chunk = ndb.get_multi([content.chunk.parent(), content.chunk])
    entities = {}
    def mark_chunk_for_update():
        entities[chunk.key] = chunk
        entities[content.key] = content
        for i, c in enumerate(stream.chunks):
            if c.chunk_id == chunk.key.id():
                stream.chunks[i] = models.ChunkInStream.from_chunk(chunk)
                entities[stream.key] = stream
                break
    delete_attachment_url = flask_extras.get_parameter('delete-attachment')
    if delete_attachment_url:
        without_deleted_attachment = lambda a: a.url != delete_attachment_url
        chunk.attachments = filter(without_deleted_attachment, chunk.attachments)
        content.attachments = chunk.attachments
        mark_chunk_for_update()
    file = request.files.get('attachment-file')
    url = flask_extras.get_parameter('attachment-url')
    if file or url:
        title = flask_extras.get_parameter('attachment-title')
        if file:
            if not title:
                title = file.filename
            path = files.upload(file.filename, file.stream, persist=True)
            url = files.storage_url(path)
        attachment = models.ChunkAttachment(title=title or url, url=url)
        # Add attachment to Chunk and ExportedContent instances.
        chunk.attachments.append(attachment)
        content.attachments = chunk.attachments
        mark_chunk_for_update()
    texts = flask_extras.get_parameter_list('sub-text')
    if texts:
        starts = flask_extras.get_parameter_list('sub-start')
        durations = flask_extras.get_parameter_list('sub-duration')
        segments = [{'text': t, 'start': int(s), 'duration': int(d)}
                    for t, s, d in zip(texts, starts, durations)
                    if t]
        if segments != content.properties['text']:
            content.properties['text'] = segments
            chunk.text_segments = [models.TextSegment(**d) for d in segments]
            mark_chunk_for_update()
    title = flask_extras.get_parameter('title')
    if title and title != content.properties.get('title'):
        content.properties['title'] = title
        entities[content.key] = content
    if entities:
        ndb.put_multi(entities.values())
    return render_template('admin_update_content.html', content=content,
                           stream=streams.Stream(stream), chunk=chunk)


@app.route('/admin/announcer', methods=['GET'])
def get_announcer():
    users = get_default_users()
    identifier = flask_extras.get_parameter('identifier')
    try:
        user = accounts.get_handler(identifier)
    except:
        user = None
    return render_template('admin_announcer.html',
                           identifier=identifier,
                           user=user, users=users)


@app.route('/admin/dashboard/convos', methods=['GET'])
def get_dashboard_convos():
    return render_template('dashboard_convos.html')


@app.route('/admin/dashboard/users', methods=['GET'])
def get_dashboard_users():
    return render_template('dashboard_users.html')


@app.route('/admin/featured', methods=['GET', 'POST'])
def get_featured():
    member = request.form.get('member')
    if member:
        user = accounts.get_handler(member)
        clone = request.form.get('clone-stream-id')
        rank = int(request.form.get('rank'))
        if clone:
            old_stream = models.Stream.set_featured(ndb.Key('Stream', int(clone)), 0)
            title = old_stream.title
            path = old_stream.image
        else:
            title = request.form.get('title')
            image = request.files.get('image')
            path = files.upload(image.filename, image.stream, persist=True)
        stream = user.streams.get_or_create([], image=path, shareable=True, title=title)
        stream.set_featured(rank)
        return 'Done! <a href="%s">Back</a>' % (request.path,)
    feature = request.form.get('feature-stream-id')
    if feature:
        rank = int(request.form.get('rank'))
        models.Stream.set_featured(ndb.Key('Stream', int(feature)), rank)
        return 'Featured! <a href="%s">Back</a>' % (request.path,)
    forget = request.form.get('forget-stream-id')
    if forget:
        stream = models.Stream.get_by_id(int(forget))
        stream.has_been_featured = False
        stream.put()
        return 'Forgotten! <a href="%s">Back</a>' % (request.path,)
    rerank = request.form.get('rerank-stream-id')
    if rerank:
        models.Stream.set_featured(ndb.Key('Stream', int(rerank)), int(request.form.get('rank')))
        return 'Reranked! <a href="%s">Back</a>' % (request.path,)
    unfeature = request.form.get('unfeature-stream-id')
    if unfeature:
        models.Stream.set_featured(ndb.Key('Stream', int(unfeature)), 0)
        return 'Unfeatured! <a href="%s">Back</a>' % (request.path,)
    q = models.Stream.query(models.Stream.has_been_featured == True)
    previously_featured = q.fetch()
    return render_template('admin_featured.html', featured=streams.get_featured(),
                           previously_featured=previously_featured)


@app.route('/admin/notify', methods=['GET'])
def get_notify():
    return render_template('admin_notify.html')


@app.route('/admin/notify', methods=['POST'])
def post_notify():
    params = {
        'app': request.form['app'],
        'env': request.form['env'],
        'text': request.form['text'],
    }
    title = request.form.get('title')
    if title:
        params['title'] = title
    tokens = re.split(r'\s+', request.files['tokens'].read().strip())
    tasks = []
    total_tasks = 0
    i, j = 0, 500
    while True:
        batch = tokens[i:j]
        if not batch:
            break
        i, j = j, j + j - i
        params['token'] = batch
        tasks.append(taskqueue.Task(method='POST', url='/_ah/jobs/notify_batch',
                                    params=params))
        if len(tasks) == taskqueue.MAX_TASKS_PER_ADD:
            taskqueue.Queue().add(tasks)
            total_tasks += len(tasks)
            tasks = []
    if tasks:
        taskqueue.Queue().add(tasks)
        total_tasks += len(tasks)
    return 'Notifying %d token(s) in %d job(s) • <a href="/admin/notify">Back</a>' % (
            len(tokens),
            total_tasks)


@app.route('/admin/run', methods=['GET'])
def get_run():
    runner = PagingJobRunner()
    job = request.args.get('job')
    if not job:
        return runner.get_job_list_html(), 200
    try:
        job_func = getattr(runner, job)
    except AttributeError:
        return runner.get_job_list_html(), 200
    try:
        job_func()
    except:
        logging.exception('Failed to run job %s', job)
        return runner.get_retry_html(), 500
    return runner.get_next_html(), 200


@app.route('/admin/services/', methods=['GET'])
def get_services():
    services = models.Service.query()
    return render_template('admin_services.html', services=services)


@app.route('/admin/services/<service_id>/', methods=['GET'])
def get_services_service_id(service_id):
    service = models.Service.get_by_id(service_id)
    q = models.ServiceAuth.query()
    q = q.filter(models.ServiceAuth.service == service.key)
    auths = q.fetch(50)
    account_keys = set(a.key.parent() for a in auths)
    accounts = {a.key: a for a in ndb.get_multi(account_keys)}
    user = collections.namedtuple('user', 'account auth')
    users = [user(accounts[a.key.parent()], a) for a in auths]
    return render_template('admin_service.html', service=service, users=users)


@app.route('/admin/services/<service_id>/teams/', methods=['GET', 'POST'])
def get_service_teams(service_id):
    data = {}
    try:
        service = models.Service.resolve_key(service_id).get()
        assert service, 'That service does not exist'
        data['service'] = service
        team_id = request.form.get('team-id')
        if team_id:
            create = request.form.get('create') == 'yes'
            team_key = models.ServiceTeam.resolve_key((service.key, team_id))
            team = team_key.get()
            if not team:
                if create:
                    team = models.ServiceTeam.create_or_update(team_key)
                else:
                    raise Exception('That team does not exist')
            return redirect('/admin/services/%s/teams/%s/' % (
                urllib.quote(service.key.id()),
                urllib.quote(team_key.id())))
    except Exception as e:
        logging.exception('Error in admin tool')
        data['error'] = e
    return render_template('admin_teams.html', **data)


@app.route('/admin/services/<service_id>/teams/<team_id>/', methods=['GET', 'POST'])
def get_service_team(service_id, team_id):
    data = {}
    try:
        team = models.ServiceTeam.resolve_key((service_id, team_id)).get()
        assert team, 'That team does not exist'
        data['service'] = team.key.parent().get()
        data['team'] = team
        image = request.files.get('image')
        if image:
            payload = files.upload(image.filename, image.stream, persist=True)
            team.image = payload
            team.put()
        if 'name' in request.form and 'slug' in request.form:
            team.name = request.form['name'] or None
            team.slug = request.form['slug'] or None
            team.whitelisted = bool(flask_extras.get_flag('whitelisted'))
            team.put()
    except Exception as e:
        logging.exception('Error in admin tool')
        data['error'] = e
    return render_template('admin_team.html', **data)


@app.route('/admin/top-youtube', methods=['GET', 'POST'])
def get_top_youtube():
    # Accounts with most aggregate YouTube views.
    accounts_by_aytv_q = models.Account.query()
    accounts_by_aytv_q = accounts_by_aytv_q.order(-models.Account.youtube_reaction_views)
    accounts_by_aytv_future = accounts_by_aytv_q.fetch_async(250)
    # Accounts with most YouTube subscribers.
    accounts_by_subs_q = models.Account.query()
    accounts_by_subs_q = accounts_by_subs_q.order(-models.Account.youtube_subs)
    accounts_by_subs_future = accounts_by_subs_q.fetch_async(250)
    # Accounts with most YouTube channel views.
    accounts_by_views_q = models.Account.query()
    accounts_by_views_q = accounts_by_views_q.order(-models.Account.youtube_channel_views)
    accounts_by_views_future = accounts_by_views_q.fetch_async(250)
    # Original content with most aggregate YouTube views.
    originals_q = models.Content.query()
    originals_q = originals_q.order(-models.Content.youtube_reaction_views)
    originals_future = originals_q.fetch_async(250)
    # Reactions with most YouTube views.
    reactions_q = models.Content.query()
    reactions_q = reactions_q.order(-models.Content.youtube_views)
    reactions_future = reactions_q.fetch_async(250)
    # Resolve all futures.
    accounts_by_aytv, accounts_by_subs, accounts_by_views, originals, reactions = (
        accounts_by_aytv_future.get_result(),
        accounts_by_subs_future.get_result(),
        accounts_by_views_future.get_result(),
        originals_future.get_result(),
        reactions_future.get_result())
    # Build a lookup map with the data we have so far.
    lookup = {}
    for entity in itertools.chain(accounts_by_aytv, accounts_by_subs, accounts_by_views, originals, reactions):
        lookup[entity.key] = entity
    # Fetch remaining missing data.
    missing_keys = set()
    for content in reactions:
        if content.creator and content.creator not in lookup:
            missing_keys.add(content.creator)
        if content.related_to and content.related_to not in lookup:
            missing_keys.add(content.related_to)
    for entity in ndb.get_multi(missing_keys):
        if not entity:
            continue
        lookup[entity.key] = entity
    # Build data sets for reactions.
    reactions = [{'creator': lookup.get(r.creator),
                  'reaction': r,
                  'related_to': lookup.get(r.related_to)}
                 for r in reactions]
    return render_template('admin_top_youtube.html',
        accounts_by_aytv=accounts_by_aytv,
        accounts_by_aytv_total=sum(a.youtube_reaction_views or 0 for a in accounts_by_aytv),
        accounts_by_subs=accounts_by_subs,
        accounts_by_subs_total=sum(a.youtube_subs or 0 for a in accounts_by_subs),
        accounts_by_views=accounts_by_views,
        accounts_by_views_total=sum(a.youtube_channel_views or 0 for a in accounts_by_views),
        originals=originals,
        reactions=reactions)


##########################################################################################
# JSON ENDPOINTS #########################################################################
##########################################################################################


@app.route('/admin/account.json', methods=['GET', 'POST'])
def get_account_json():
    API_VERSION = 53
    identifier = request.args.get('identifier')
    if identifier:
        account = models.Account.resolve(identifier)
        if not account:
            return 'null', 404
    else:
        account = None
    if request.method == 'GET':
        if account:
            result = account
        else:
            # Support batch request.
            identifiers = request.args.get('identifiers')
            if not identifiers:
                return 'null', 404
            result = models.Account.resolve_list(identifiers.split(','))
        return convert.to_json(result, include_extras=True, include_identifiers=True, version=API_VERSION)
    def mapper(key, value):
        if key == 'verified':
            if value not in ('false', 'true'):
                raise ValueError('Invalid boolean')
            return value == 'true'
        return value
    errors = []
    values = form_to_dict(mapper)
    # Create/update account.
    if account:
        # Handler related updates (business logic).
        handler = accounts.get_handler(account)
        attrs = {}
        for key, value in values.iteritems():
            try:
                if key == 'display_name':
                    handler.set_display_name(value)
                elif key == 'identifier':
                    handler.set_username(value)
                elif key == 'status':
                    handler.change_status(value, status_reason='admin_tool')
                else:
                    attrs[key] = value
            except Exception as e:
                logging.exception('Error setting %r to %r:', key, value)
                errors.append(e)
                continue
        # Update the model directly.
        account = handler.account
        if not account.properties:
            account.properties = {}
        for key, value in attrs.iteritems():
            if key == 'properties':
                for k, v in value.iteritems():
                    if v is None:
                        account.properties.pop(k, None)
                    else:
                        account.properties[k] = v
            else:
                setattr(account, key, value)
        if attrs:
            account.put()
    else:
        try:
            account = models.Account.create(**values)
        except Exception as e:
            logging.exception('Error creating account %r:', values)
            errors.append(e)
    if errors:
        return convert.to_json({'errors': map(str, errors)}, version=API_VERSION), 500
    return convert.to_json(account, include_extras=True, version=API_VERSION)


@app.route('/admin/clear-cache.json', methods=['POST'])
def post_clear_cache_json():
    memcache.delete(request.args.get('cache_key'))
    return '{}'


@app.route('/admin/content.json', methods=['GET', 'POST'])
def get_content_json():
    identifier = request.args.get('identifier')
    if not isinstance(identifier, basestring) or not identifier:
        return '{}', 404
    if re.match(r'^\d+$', identifier):
        content_id = int(identifier)
        content = models.Content.get_by_id(content_id)
    else:
        url, _ = utils.normalize_content_urls(identifier, None)
        content = models.Content.query(models.Content.original_url == url).get()
    if not content:
        return '{}', 404
    if request.method == 'POST':
        # Convert form values into model values and set them on the content.
        if not content.properties:
            content.properties = {}
        def mapper(key, value):
            if key == 'creator':
                value = ndb.Key('Account', int(value))
            if key.startswith('properties.'):
                key = key[11:]
                if value is None:
                    content.properties.pop(key, None)
                else:
                    content.properties[key] = value
            else:
                setattr(content, key, value)
        form_to_dict(mapper)
        content.put()
    return convert.to_json(content, version=53)


@app.route('/admin/accounts/create.json', methods=['POST'])
def post_accounts_create_json():
    identifier, identifier_type = identifiers.parse(request.form['identifier'])
    if identifier_type not in (identifiers.EMAIL, identifiers.SERVICE_ID, identifiers.USERNAME):
        raise Exception('Invalid identifier %s' % request.form['identifier'])
    params = dict(status='invited')
    status = flask_extras.get_parameter('status')
    if status:
        params['status'] = status
    handler = accounts.get_or_create(identifier, **params)
    display_name = request.form.get('display_name')
    if display_name and handler.display_name != display_name:
        handler.set_display_name(display_name)
    image = request.files.get('image')
    if image:
        payload = files.upload(image.filename, image.stream, persist=True)
        handler.set_image(payload)
    data = handler.public(version=53)
    return convert.to_json(data)


@app.route('/admin/announcer/announce.json', methods=['POST'])
def post_announcer_announce():
    successful = 0
    failed = 0
    payload = flask_extras.get_parameter('payload')
    duration = int(flask_extras.get_parameter('duration'))
    streams_handler = streams.get_handler(flask_extras.get_parameter('sender_identifier'))
    identifiers = flask_extras.get_parameter_list('identifier')
    for identifier in identifiers:
        try:
            streams_handler.send([identifier], payload, duration)
            successful += 1
            logging.debug('Sent to {}'.format(identifier))
        except Exception as e:
            logging.error('Failed to send to {} ({})'.format(identifier, e))
            failed += 1
    return convert.to_json({
        'successful': successful,
        'failed': failed,
    })


@app.route('/admin/content/add_tag.json', methods=['POST'])
def post_content_add_tag_json():
    try:
        content_id = int(flask_extras.get_parameter('id'))
        tags = flask_extras.get_parameter_list('tag')
    except:
        return '{"error": "Invalid input"}', 400
    content = models.Content.get_by_id(content_id)
    if not content:
        return '{"error": "Not found"}', 404
    content.add_tags(tags, allow_restricted=True)
    content.put()
    return '{"success": true}'


@app.route('/admin/comments.json', methods=['GET'])
def get_comments_json():
    content_id = int(request.args['content_id'])
    q = models.ContentComment.query(ancestor=ndb.Key('Content', content_id))
    q = q.order(-models.ContentComment.created)
    comments = q.fetch()
    lookup = {a.key: a for a in ndb.get_multi({c.creator for c in comments})}
    data = [c.public(creator=lookup[c.creator], version=53) for c in comments]
    return convert.to_json({'data': data}, version=53)


@app.route('/admin/comments.json', methods=['DELETE'])
def post_comments_json():
    content_id = int(request.args['content_id'])
    key = ndb.Key('Content', content_id,
                  'ContentComment', request.args['comment_id'])
    key.delete()
    futures = []
    v = 51
    c = ndb.get_context()
    for x in xrange(3):
        futures += [
            c.memcache_delete('content_comments_%s_%d_created' % (v - x, content_id)),
            c.memcache_delete('content_comments_%s_%d_offset' % (v - x, content_id)),
            c.memcache_delete('content_comments_%s_%d_threaded' % (v - x, content_id)),
        ]
    ndb.Future.wait_all(futures)
    return convert.to_json({'success': True})


@app.route('/admin/decorate_content.json', methods=['GET'])
def get_decorate_content():
    content_id = int(request.args['content_id'])
    content = models.Content.get_by_id(content_id)
    removed_tags = filter(lambda t: t not in content.tags, content.tags_history)
    return convert.to_json(dict(content.public(version=43), removed_tags=removed_tags))


@app.route('/admin/events.json', methods=['GET'])
def get_events():
    account_key = ndb.Key('Account', int(request.args.get('account_id')))
    q = models.AccountEvent.query(ancestor=account_key)
    q = q.order(-models.AccountEvent.timestamp)
    cursor = datastore_query.Cursor(urlsafe=request.args.get('cursor'))
    events, next_cursor, more = q.fetch_page(200, start_cursor=cursor)
    expanded_events = sorted(itertools.chain.from_iterable(e.items for e in events),
                             key=lambda i: i.timestamp, reverse=True)
    result = {
        'cursor': next_cursor.urlsafe() if more else None,
        'data': expanded_events,
    }
    return convert.to_json(result)


@app.route('/admin/announcer/prepare.json', methods=['POST'])
def post_announcer_prepare():
    payload = request.files.get('payload')
    path = files.upload(payload.filename, payload.stream, persist=True)
    return convert.to_json({'path': path})


@app.route('/admin/dashboard/convos.json', methods=['GET'])
def get_dashboard_convos_json():
    row = bigquery_client.query(QUERY_DASHBOARD_CONVOS).rows().next()
    data = {k: int(v) for k, v in row._asdict().iteritems()}
    return convert.to_json(data)


@app.route('/admin/dashboard/users.json', methods=['GET'])
def get_dashboard_users_json():
    data = {}
    for row in bigquery_client.query(QUERY_DASHBOARD_USERS).rows():
        data[row.key] = int(row.users)
    return convert.to_json(data)


@app.route('/admin/push-info.json', methods=['GET'])
def get_push_info():
    account_key = ndb.Key('Account', int(request.args.get('account_id')))
    devices = [d.to_dict() for d in models.Device.query(ancestor=account_key).fetch()]
    return convert.to_json({'devices': devices})


@app.route('/admin/query/release-link-activity.json', methods=['GET'])
def get_query_release_link_activity_json():
    days_ago = timedelta(days=int(request.args['days_ago']))
    # Calculate time range to get ranks for.
    utc_now = pytz.utc.localize(datetime.utcnow())
    midnight = utc_now.astimezone(pytz.timezone('America/New_York'))
    midnight = midnight.replace(hour=0, minute=0, second=0, microsecond=0)
    midnight = midnight.astimezone(pytz.utc)
    midnight -= days_ago
    ts_min = midnight.strftime('%Y-%m-%d %H:%M')
    midnight += timedelta(days=1)
    ts_max = midnight.strftime('%Y-%m-%d %H:%M')
    # First, try to get the data from cache.
    if midnight < utc_now:
        cache_key = 'admin_release_activity_%s_%s' % (ts_min, ts_max)
        json_data = memcache.get(cache_key)
        if json_data:
            return json_data
    else:
        cache_key = None
    # Get the data from BigQuery.
    q = QUERY_RELEASE_LINK_ACTIVITY % (ts_min, ts_max, 1000)
    data = collections.defaultdict(lambda: collections.defaultdict(dict))
    for row in bigquery_client.query(q).rows():
        item = {'count': int(row.count), 'unique_count': int(row.unique_count)}
        data[str(row.creator_id)][str(row.content_id)][row.activity_type] = item
    json_data = convert.to_json({'data': data})
    if cache_key:
        memcache.set(cache_key, json_data, time=604800)
    return json_data


@app.route('/admin/search-evict.json', methods=['POST'])
def post_search_evict():
    content_id = flask_extras.get_parameter('id')
    query = flask_extras.get_parameter('query')
    # TODO: Consider clearing memcache for search query.
    search.Index('original2').delete(content_id)
    return '{}'


@app.route('/admin/send-email.json', methods=['POST'])
def post_send_email():
    args = json.loads(request.form['args'])
    assert isinstance(args, list)
    kwargs = json.loads(request.form['kwargs'])
    assert isinstance(kwargs, dict)
    localize.send_email(*args, **kwargs)
    return '{}'


@app.route('/admin/service-info.json', methods=['GET'])
def get_service_info():
    account_key = ndb.Key('Account', int(request.args.get('account_id')))
    services = []
    for auth in models.ServiceAuth.query(ancestor=account_key):
        data = auth.to_dict(exclude=['service', 'service_clients', 'service_team'])
        if auth.service_team:
            service, team = ndb.get_multi([auth.service, auth.service_team])
        else:
            service, team = auth.service.get(), None
        data['id'] = service.key.id()
        data['team'] = {'id': team.key.id(), 'name': team.name} if team else None
        data['image_url'] = service.image_url
        data['title'] = service.title
        services.append(data)
    return convert.to_json({'services': services})


@app.route('/admin/services/<service_id>/teams.json', methods=['GET'])
def get_service_teams_json(service_id):
    service_key = models.Service.resolve_key(service_id)
    q = models.ServiceTeam.query(ancestor=service_key)
    q = q.order(-models.ServiceTeam.created)
    cursor = datastore_query.Cursor(urlsafe=request.args.get('cursor'))
    teams, next_cursor, more = q.fetch_page(100, start_cursor=cursor)
    result = {
        'cursor': next_cursor.urlsafe() if more else None,
        'data': map(lambda t: {'id': t.key.id(), 'created': t.created, 'name': t.name, 'whitelisted': t.whitelisted}, teams),
    }
    return convert.to_json(result)


@app.route('/admin/services/<service_id>/teams/<team_id>/create.json', methods=['POST'])
def post_service_team_create_json(service_id, team_id):
    team = models.ServiceTeam.resolve_key((service_id, team_id)).get()
    assert team, 'That team does not exist'
    email = request.form['identifier']
    identifier, identifier_type = identifiers.parse(email)
    if identifier_type != identifiers.EMAIL:
        raise Exception('Invalid email %s' % email)
    _, t, _ = models.Service.parse_identifier(identifier)
    if t != team.key:
        raise Exception('Wrong team for %s' % email)
    invited = accounts.get_or_create(identifier, status='invited')
    display_name = request.form.get('display_name')
    if display_name and invited.display_name != display_name:
        invited.set_display_name(display_name)
    image = request.files.get('image')
    if image:
        payload = files.upload(image.filename, image.stream, persist=True)
        invited.set_image(payload)
    if flask_extras.get_flag('send_invite'):
        localize.send_email('fika', 'generic_invite',
            to=identifiers.email(identifier),
            to_name=invited.display_name,
            invited_name=invited.display_name,
            team_name=team.name,
            team_slug=team.slug_with_fallback)
    return convert.to_json(invited, version=30)


@app.route('/admin/services/<service_id>/teams/<team_id>/members.json', methods=['GET'])
def get_service_team_members_json(service_id, team_id):
    cursor = datastore_query.Cursor(urlsafe=request.args.get('cursor'))
    team_key = models.ServiceTeam.resolve_key((service_id, team_id))
    q = models.ServiceAuth.query(models.ServiceAuth.service_team == team_key)
    auths, next_cursor, more = q.fetch_page(100, start_cursor=cursor)
    accounts = ndb.get_multi(map(lambda auth: auth.key.parent(), auths))
    def record(auth, account):
        data = dict(auth.to_dict(include=['last_refresh', 'properties', 'service_identifier']),
                    **account.public(version=29))
        data['clients'] = map(lambda c: c.id(), auth.service_clients)
        return data
    result = {
        'cursor': next_cursor.urlsafe() if more else None,
        'data': map(lambda z: record(*z), zip(auths, accounts)),
    }
    return convert.to_json(result)


@app.route('/admin/services/<service_id>/teams/<team_id>/pending.json', methods=['GET'])
def get_service_team_pending_json(service_id, team_id):
    start = identifiers.build_service(service_id, team_id, '')
    start_key = ndb.Key('Identity', start)
    end = start[:-1] + chr(ord(start[-1]) + 1)
    end_key = ndb.Key('Identity', end)
    q = models.Identity.query()
    q = q.filter(models.Identity.key >= start_key)
    q = q.filter(models.Identity.key < end_key)
    account_keys = [i.account for i in q.fetch() if i.account and not i.is_activated]
    accounts = ndb.get_multi(account_keys)
    return convert.to_json({'data': accounts}, version=30)


@app.route('/admin/trending_content.json', methods=['GET'])
def get_trending_content_json():
    cache_key = 'admin_trending_content'
    result_json = memcache.get(cache_key)
    if result_json:
        logging.debug('Loaded cache key %r', cache_key)
        return result_json
    rows = bigquery_client.query(QUERY_CONTENT_TRENDING).rows()
    items, item = [], None
    for row in rows:
        content_id = int(row.content_id)
        if not item or item[0] != content_id:
            if item:
                items.append(item)
            item = [content_id, 0, [0] * 49]
        hour = int(row.h)
        reactions = int(row.reactions)
        if hour >= 24: item[1] += reactions
        item[2][hour] = reactions
    if item:
        items.append(item)
    cache_ttl = 120
    result_json = convert.to_json({'data': items})
    memcache.set(cache_key, result_json, time=cache_ttl)
    logging.debug('Saved to cache key %r (ttl: %d)', cache_key, cache_ttl)
    return result_json


@app.route('/admin/wallets.json', methods=['GET'])
def get_wallets_json():
    account_id = request.args.get('account_id')
    request_id = request.args.get('request_id')
    wallets = []
    if account_id:
        account_key = ndb.Key('Account', int(account_id))
        q = models.Wallet.query()
        q = q.filter(models.Wallet.account == account_key)
        q = q.order(-models.Wallet.updated)
        wallets = q.fetch()
    elif request_id:
        r = models.ContentRequestPublic.get_by_id(int(request_id))
        if r.wallet:
            wallets.append(r.wallet.get())
    return convert.to_json({
        'wallets': [dict(w.to_dict(exclude=['account']), id=w.key.id()) for w in wallets],
    })


@app.route('/admin/wallet_transactions.json', methods=['GET'])
def get_wallet_transactions_json():
    wallet_key = ndb.Key('Wallet', request.args.get('wallet_id'))
    q = models.WalletTransaction.query(ancestor=wallet_key)
    q = q.order(-models.WalletTransaction.timestamp)
    txs = q.fetch()
    account_keys = set(itertools.chain.from_iterable((tx.receiver, tx.sender) for tx in txs))
    entities = ndb.get_multi(account_keys)
    lookup = {e.key: e for e in entities if e}
    items = []
    for tx in txs:
        i = dict(tx.to_dict(exclude=['other_tx']),
                 id=tx.key.id(),
                 receiver=lookup.get(tx.receiver),
                 receiver_wallet_id=tx.receiver_wallet.id(),
                 sender=lookup.get(tx.sender),
                 sender_wallet_id=tx.sender_wallet.id())
        items.append(i)
    return convert.to_json({'transactions': items}, version=53)


@ndb.tasklet
def _add_task_async(task, **kwargs):
    yield task.add_async(**kwargs)
