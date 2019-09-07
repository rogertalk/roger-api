# -*- coding: utf-8 -*-

from collections import namedtuple
from datetime import datetime
from HTMLParser import HTMLParser
import json
import logging
import re

from google.appengine.api import taskqueue, urlfetch
from google.appengine.ext import ndb

import feedparser
from flask import Flask, request

from roger import accounts, config, files, models, slack_api
from roger.apps import utils
from roger_common import bigquery_api, convert, flask_extras


app = Flask(__name__)
app.config['DEBUG'] = config.DEVELOPMENT


html_parser = HTMLParser()


class MetadataExtractor(HTMLParser):
    Matcher = namedtuple('Matcher', 'tag match_attr match_value value_attr')

    MATCHERS = {
        'image': [
            Matcher('link', 'rel', 'image_url', 'href'),
            Matcher('meta', 'itemprop', 'thumbnailUrl', 'content'),
            Matcher('meta', 'name', 'twitter:image', 'content'),
            Matcher('meta', 'property', 'og:image', 'content'),
        ],
        'title': [
            Matcher('meta', 'property', 'og:title', 'content'),
            Matcher('meta', 'name', 'twitter:title', 'content'),
            #Matcher('title', None, None, None),
        ],
        'url': [
            Matcher('meta', 'property', 'og:url', 'content'),
        ],
        'youtube_embed': [
            Matcher('iframe', 'src', lambda v: v.startswith('https://www.youtube.com/'), 'src')
        ],
    }

    def __init__(self):
        HTMLParser.__init__(self)
        self.image = None
        self.title = None
        self.url = None
        self.youtube_embed = None

    def handle_starttag(self, tag, attrs):
        tag = tag.lower()
        attrs_dict = {k.lower(): v for k, v in attrs}
        for prop, matchers in self.MATCHERS.iteritems():
            for matcher in matchers:
                if getattr(self, prop) is not None:
                    continue
                if matcher.tag and matcher.tag != tag:
                    continue
                if matcher.match_attr:
                    if matcher.match_attr not in attrs_dict:
                        continue
                    match_value = attrs_dict[matcher.match_attr]
                    if callable(matcher.match_value):
                        if not matcher.match_value(match_value):
                            continue
                    else:
                        if matcher.match_value != match_value.lower():
                            continue
                value = attrs_dict.get(matcher.value_attr)
                setattr(self, prop, value)


QUERY_A0_GROUP_STATS = """
SELECT
  client,
  COUNT(DISTINCT identifier) AS num_activations,
  ROUND(SUM(IF(convo_type = 3, 1, 0)) / COUNT(DISTINCT identifier) * 100, 1) AS pct_group,
FROM (-- Distinct combinations of date, identifier, and conversation type.
  SELECT
    client,
    identifier,
    convo_type
  FROM (-- Users who activated and an associated conversation type.
    SELECT
      client,
      identifier,
      IF(num_participants > 2, 3, num_participants) AS convo_type
    FROM (-- Users who activated and spoke within 24 hours.
      SELECT
        CASE
          WHEN a.client = 'CFNetwork' THEN 'ios'
          WHEN a.client IN ('android', 'ios') THEN a.client
          ELSE 'other'
        END AS client,
        a.identifier AS identifier,
        COUNT(s.participant_ids) WITHIN RECORD AS num_participants
      FROM
        roger_reporting.account_activated_v2 AS a
      JOIN roger_reporting.stream_v2 AS s ON s.identifier = a.identifier
      WHERE
        DATE(a.timestamp) = DATE(DATE_ADD(CURRENT_TIMESTAMP(), -2, 'DAY'))
        AND TIMESTAMP_TO_SEC(s.timestamp) - TIMESTAMP_TO_SEC(a.timestamp) < 86400
        AND s.status = 'sent' ) )
  GROUP BY 1, 2, 3 )
GROUP BY 1
ORDER BY 1
"""


QUERY_DAILY_TALKING_STATS = """
SELECT
  COUNT(DISTINCT account_id) AS people,
  ROUND(SUM(duration) / 3600, 1) AS total_hours
FROM roger_reporting.stream_v3
WHERE
  account_status = 'active'
  AND DATE(timestamp) = DATE(DATE_ADD(CURRENT_TIMESTAMP(), -1, 'DAY'))
  AND status IN ('played', 'sent')
"""


QUERY_DAU_GROUP_STATS = """
SELECT
  client,
  COUNT(DISTINCT identifier) AS num_talkers,
  SUM(IF(convo_type = 1, 1, 0)) AS num_solo,
  ROUND(SUM(IF(convo_type = 1, 1, 0)) / COUNT(DISTINCT identifier) * 100, 1) AS pct_solo,
  SUM(IF(convo_type = 2, 1, 0)) AS num_duo,
  ROUND(SUM(IF(convo_type = 2, 1, 0)) / COUNT(DISTINCT identifier) * 100, 1) AS pct_duo,
  SUM(IF(convo_type = 3, 1, 0)) AS num_group,
  ROUND(SUM(IF(convo_type = 3, 1, 0)) / COUNT(DISTINCT identifier) * 100,1) AS pct_group
FROM (
  SELECT
    client,
    identifier,
    convo_type
  FROM (-- Users who spoke and an associated conversation type.
    SELECT
      client,
      identifier,
      IF(num_participants > 2, 3, num_participants) AS convo_type
    FROM (-- Users who spoke in the past 24 hours.
      SELECT
        CASE
          WHEN client = 'CFNetwork' THEN 'ios'
          WHEN client IN ('android', 'ios') THEN client
          ELSE 'other'
        END AS client,
        identifier,
        COUNT(participant_ids) WITHIN RECORD AS num_participants
      FROM
        roger_reporting.stream_v2
      WHERE
        status = 'sent'
        AND DATE(timestamp) = DATE(DATE_ADD(CURRENT_TIMESTAMP(), -1, 'DAY')) ) )
  GROUP BY 1, 2, 3 )
GROUP BY 1
ORDER BY 1
"""


QUERY_REACTED_CONTENT = """
SELECT
  related_id AS content_id,
  COUNT(*) AS reactions
FROM
  roger_reporting.content_v2
WHERE
  related_id IS NOT NULL
  AND timestamp >= DATE_ADD(CURRENT_TIMESTAMP(), -48, 'HOUR')
GROUP BY
  1
ORDER BY
  2 DESC
"""


QUERY_TOP_15_TALKERS_PAST_WEEK = """
SELECT
  account_id,
  COUNT(*) AS chunks_sent,
  COUNT(DISTINCT stream_id) AS unique_convos,
  CAST((SUM(duration) / 60) AS INTEGER) AS total_minutes,
  ROUND(SUM(duration) / COUNT(*), 1) AS seconds_per_chunk,
  GROUP_CONCAT(UNIQUE(client)) AS clients
FROM roger_reporting.stream_v3
WHERE
  account_status = 'active'
  AND DATE(timestamp) >= DATE(DATE_ADD(CURRENT_TIMESTAMP(), -7, 'DAY'))
  AND DATE(timestamp) < CURRENT_DATE()
  AND status = 'sent'
GROUP BY account_id
ORDER BY total_minutes DESC
LIMIT 15
"""


QUERY_TOP_500_TALKERS_PAST_WEEK = """
SELECT
  account_id,
  SUM(duration) AS total_seconds
FROM roger_reporting.stream_v3
WHERE
  account_status = 'active'
  AND DATE(timestamp) >= DATE(DATE_ADD(CURRENT_TIMESTAMP(), -7, 'DAY'))
  AND DATE(timestamp) < CURRENT_DATE()
  AND status = 'sent'
GROUP BY account_id
ORDER BY total_seconds DESC
LIMIT 500
"""


bigquery_client = bigquery_api.BigQueryClient.for_appengine(
    project_id=config.BIGQUERY_PROJECT,
    dataset_id=config.BIGQUERY_DATASET,
    )


@app.route('/_ah/cron/create_deletion_jobs', methods=['GET'])
def delete_expired_chunks():
    # The latest possible timestamp for expired chunks.
    delete_before = convert.unix_timestamp(datetime.utcnow() - config.CHUNK_MAX_AGE)
    taskqueue.add(
        method='GET',
        url='/_ah/jobs/delete_chunks',
        params={'delete_before': delete_before},
        queue_name=config.DELETE_CHUNKS_QUEUE_NAME)
    return ''


@app.route('/_ah/cron/good_afternoon_slack', methods=['GET'])
def good_afternoon_slack():
    """Send a good afternoon message to our #roger channel every 24 hours."""
    android, ios = 0, 0
    for row in bigquery_client.query(QUERY_DAU_GROUP_STATS).rows():
        if row.client == 'android':
            android = float(row.pct_group)
        elif row.client == 'ios':
            ios = float(row.pct_group)
    # Send a message to #roger.
    message = 'Good afternoon, hope you had a great lunch!\n\n'
    message += 'Out of everyone who spoke yesterday, '
    message += '*{}%* spoke to groups on Android. '.format(android)
    message += 'On iOS, *{}%* of everyone spoke to groups.'.format(ios)
    slack_api.message(channel='#roger', text=message, defer=False)
    return ''


@app.route('/_ah/cron/good_evening_slack', methods=['GET'])
def good_evening_slack():
    """Send a good evening message to our #roger channel every 24 hours."""
    android, ios, total_a0 = 0, 0, 0
    for row in bigquery_client.query(QUERY_A0_GROUP_STATS).rows():
        total_a0 += int(row.num_activations)
        if row.client == 'android':
            android = float(row.pct_group)
        elif row.client == 'ios':
            ios = float(row.pct_group)
    # Send a message to #roger.
    message = 'Good evening! Yesterday '
    message += '*{:,}* people signed up and talked '.format(total_a0)
    message += 'for the first time in Roger! '
    message += '*{}%* of the Android users spoke to a group, and '.format(android)
    message += '*{}%* of the iOS users spoke to a group.'.format(ios)
    slack_api.message(channel='#roger', text=message, defer=False)
    return ''


@app.route('/_ah/cron/good_morning_slack', methods=['GET'])
def good_morning_slack():
    """Send a good morning message to our #roger channel every 24 hours."""
    row = bigquery_client.query(QUERY_DAILY_TALKING_STATS).rows().next()
    # Send a message to #roger.
    message = 'Good morning everyone!\n\nYesterday '
    message += '*{:,}* people talked/listened for *{} hours*.'.format(int(row.people),
                                                                      row.total_hours)
    slack_api.message(channel='#roger', text=message, defer=False)
    return ''


@app.route('/_ah/cron/happy_friday_slack', methods=['GET'])
def happy_friday_slack():
    """Post weekly stats on Slack."""
    attachment = slack_api.attachment(
        'Top 15 talkers last week',
        pretext='Happy Friday! Some of last week\'s top talkers:',
        fields=[])
    rows = list(bigquery_client.query(QUERY_TOP_15_TALKERS_PAST_WEEK).rows())
    accounts = ndb.get_multi(ndb.Key('Account', int(r.account_id)) for r in rows)
    for i, row in enumerate(rows):
        account = accounts[i]
        name = account.display_name.split(' ')[0]
        if account.location_info:
            name += u' in {}, {}'.format(account.location_info.city,
                                         account.location_info.country)
        minutes = float(row.total_minutes)
        if minutes < 60:
            duration = '{:.1f} minutes'.format(minutes)
        else:
            duration = '{:.1f} hours'.format(minutes / 60)
        num_people = int(row.unique_convos)
        sentence = 'Talked for {} to {} (averaging {} seconds at a time)'.format(
            duration,
            ('{} person' if num_people == 1 else '{} people').format(num_people),
            row.seconds_per_chunk)
        attachment['fields'].append(slack_api.field(name, sentence))
    slack_api.message(channel='#roger', attachments=[attachment], defer=False)
    return ''


@app.route('/_ah/cron/import_content', methods=['GET'])
def import_content():
    futures = map(_import_subreddit_async, config.SUGGESTION_POOL_SUBREDDITS)
    content_created = 0
    for f in futures:
        try:
            content_created += len(f.get_result())
        except:
            logging.exception('Failed to import new content.')
    logging.debug('Matched %d Content objects from external sources.', content_created)
    return ''


@app.route('/_ah/cron/report_to_bigquery', methods=['GET', 'POST'])
def report_to_bigquery():
    """Flush all pending events of a certain type to BigQuery."""
    # Schedule multiple flush jobs per minute for some events.
    if request.method == 'GET':
        tasks = []
        for delay in xrange(0, 60, 5):
            tasks.append(taskqueue.Task(method='POST', url=request.path,
                                        countdown=delay,
                                        params={'event_name': 'content_vote_v1'}))
        tasks.append(taskqueue.Task(method='POST', url=request.path))
        taskqueue.Queue(config.BIGQUERY_CRON_QUEUE_NAME).add(tasks)
        return ''
    # Retrieve pending events from pull queue.
    try:
        q = taskqueue.Queue(config.BIGQUERY_QUEUE_NAME)
        tasks = q.lease_tasks_by_tag(config.BIGQUERY_LEASE_TIME.total_seconds(),
                                     config.BIGQUERY_LEASE_AMOUNT,
                                     tag=flask_extras.get_parameter('event_name'))
        logging.debug('Leased %d event(s) from %s', len(tasks), config.BIGQUERY_QUEUE_NAME)
    except taskqueue.TransientError:
        logging.warning('Could not lease events due to transient error')
        return '', 503
    if not tasks:
        return ''
    # Insert the events into BigQuery.
    table_id = tasks[0].tag
    rows = [json.loads(t.payload) for t in tasks]
    bigquery_client.insert_rows(table_id, rows)
    # Delete the tasks now that we're done with them.
    q.delete_tasks(tasks)
    return ''


@app.route('/_ah/cron/update_content_requests', methods=['GET'])
def update_content_requests():
    tags = {'approved', 'default'}
    q = models.ContentRequestPublic.query()
    for tag in tags:
        q = q.filter(models.ContentRequestPublic.tags == tag)
    q = q.order(-models.ContentRequestPublic.sort_index)
    futures = []
    delay = 0
    for request in q:
        # TODO: Improved condition to avoid checking depleted requests.
        # TODO: Add way to manually update content request entries when request is closed.
        if not request.wallet or request.closed:
            continue
        task = taskqueue.Task(
            countdown=delay,
            url='/_ah/jobs/update_content_request_entries',
            params={
                'request_id': str(request.key.id()),
                'wallet_id': str(request.wallet.id()),
                'wallet_owner_id': str(request.wallet_owner.id()),
            },
            retry_options=taskqueue.TaskRetryOptions(task_retry_limit=0))
        futures.append(_add_task_async(task, queue_name=config.INTERNAL_QUEUE))
        delay += 5
    _wait_all(futures)
    logging.debug('Scheduled %d job(s) to update content request entries', len(futures))
    return ''


@app.route('/_ah/cron/update_top_creators', methods=['GET'])
def update_top_creators():
    account_keys = [ndb.Key('Account', aid) for aid in config.TOP_CREATOR_IDS]
    utils.recalculate_content_reaction_counts(account_keys)
    return ''


@app.route('/_ah/cron/update_youtube_stats', methods=['GET'])
def update_youtube_stats():
    futures = []
    delay = 0
    for row in bigquery_client.query(QUERY_REACTED_CONTENT).rows():
        task = taskqueue.Task(
            countdown=delay,
            url='/_ah/jobs/update_youtube_views_batched',
            params={'original_id': row.content_id},
            retry_options=taskqueue.TaskRetryOptions(task_retry_limit=0))
        futures.append(_add_task_async(task, queue_name=config.INTERNAL_QUEUE))
        delay += 2
    _wait_all(futures)
    return ''


@ndb.tasklet
def _add_task_async(task, **kwargs):
    yield task.add_async(**kwargs)


@ndb.tasklet
def _import_subreddit_async(subreddit):
    context = ndb.get_context()
    try:
        result = yield context.urlfetch(
            method=urlfetch.GET,
            url='https://www.reddit.com/r/%s/hot.json?limit=50' % (subreddit,))
        data = json.loads(result.content)
        posts = data['data']['children']
    except urlfetch.ConnectionClosedError:
        logging.warning('Failed to connect to www.reddit.com')
        raise ndb.Return([])
    except Exception as e:
        logging.exception('Fetching /r/%s data failed.' % (subreddit,))
        raise ndb.Return([])
    futures = []
    for post in posts:
        post_id = post['data']['id']
        media = post['data']['media']
        if not media or 'oembed' not in media:
            logging.debug('Skipping %s.media: %r', post_id, media)
            continue
        info = media['oembed']
        provider = info['provider_name']
        if provider in ('BandCamp', 'Imgur', 'SoundCloud', 'Spotify'):
            logging.debug('Skipping %s.media for %s', post_id, provider)
            continue
        if provider == 'Streamable':
            m = re.search(r'&amp;url=https%3A%2F%2Fstreamable\.com%2F([0-9a-z]+)&amp;', info['html'])
            if not m:
                logging.debug('Failed to get Streamable id from %s.media %r', post_id, media)
                continue
            title = html_parser.unescape(post['data']['title'])
            url = 'https://streamable.com/%s' % (m.group(1),)
        elif provider == 'YouTube':
            m = re.search(r'https(?:://|%3A%2F%2F)www\.youtube\.com(?:/|%2F)embed(?:/|%2F)([a-zA-Z0-9_-]+)', info['html'])
            if not m:
                logging.debug('Failed to get YouTube id from %s.media %r', post_id, media)
                continue
            title = html_parser.unescape(info['title'])
            url = 'https://www.youtube.com/watch?v=%s' % (m.group(1),)
        else:
            logging.debug('Skipping %s.media for %s: %r', post_id, provider, media)
            continue
        future = utils.get_or_create_content_async(
            url=url,
            thumb_url=info['thumbnail_url'],
            title=title,
            tags=['original', 'is suggestion'],
            allow_restricted_tags=True)
        futures.append(future)
    content_list = []
    for f in futures:
        try:
            content = yield f
        except:
            logging.exception('Failed to create content.')
        content_list.append(content)
    raise ndb.Return(content_list)


@ndb.synctasklet
def _wait_all(futures):
    errors = 0
    for f in futures:
        try:
            yield f
        except Exception:
            logging.exception('Error in Future')
            errors += 1
    if errors:
        raise Exception('%d error(s) occurred' % (errors,))
