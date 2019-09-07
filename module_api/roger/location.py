# -*- coding: utf-8 -*-

from collections import defaultdict
from datetime import datetime, timedelta
import hashlib
import json
import logging
import math
from random import randint
import re
import time

from google.appengine.api import memcache, urlfetch
from google.appengine.ext import ndb

from werkzeug.contrib.cache import SimpleCache

from roger import config

# used in local thread
_local_cache = SimpleCache(threshold=500, default_timeout=300)


def _cache_get(key):
    data = _local_cache.get(key)
    if data:
        return data
    return memcache.get(key)


def _cache_set(key, value, ttl):
    _local_cache.set(key, value, ttl)
    memcache.set(key, value, time=ttl)


API_BASE = 'https://maps.googleapis.com/maps/api'
GEOCODE_URL = API_BASE + '/geocode/json?latlng={latlng}&key={api_key}'
TIME_ZONE_URL = API_BASE + '/timezone/json?location={latlng}&timestamp={timestamp}&key={api_key}'
WEATHER_URL = 'https://api.forecast.io/forecast/{api_key}/{latlng}?exclude=hourly,alerts,flags&units=si'


def api_callback(rpc, cache_key, cache_duration):
    data = api_json(rpc)
    if not data:
        return
    _cache_set(cache_key, dict(data=data, timestamp=datetime.utcnow()), cache_duration)


def api_json(rpc):
    result = rpc.get_result()
    if not 200 <= result.status_code < 300:
        logging.warning('API responded with HTTP {}'.format(result.status_code))
        logging.warning(result.content)
        return None
    try:
        return json.loads(result.content)
    except:
        logging.exception('Failed to parse request body as JSON')
        logging.warning(result.content)


def api_request(url, cache_duration=604800):
    """Returns a function that when called results in parsed JSON data or None."""
    cache_key = 'location/v2:{}'.format(hashlib.sha256(url).hexdigest())
    # Attempt to get a cached response before calling the API.
    data = _cache_get(cache_key)
    # Random TTL is used to avoid making multiple requests to API when expiring
    ttl = timedelta(seconds=cache_duration - randint(0, 300))
    if data and data['timestamp'] + ttl > datetime.utcnow():
        return lambda: data['data']
    # Perform the request and update memcache on success.
    logging.debug('API request {}'.format(url))
    rpc = urlfetch.create_rpc()
    rpc.callback = lambda: api_callback(rpc, cache_key, cache_duration)
    urlfetch.make_fetch_call(rpc, url)
    return lambda: api_json(rpc)


flags = {
    'AD': u'\U0001F1E6\U0001F1E9',
    'AE': u'\U0001F1E6\U0001F1EA',
    'AF': u'\U0001F1E6\U0001F1EB',
    'AG': u'\U0001F1E6\U0001F1EC',
    'AI': u'\U0001F1E6\U0001F1EE',
    'AL': u'\U0001F1E6\U0001F1F1',
    'AM': u'\U0001F1E6\U0001F1F2',
    'AO': u'\U0001F1E6\U0001F1F4',
    'AQ': u'\U0001F1E6\U0001F1F6',
    'AR': u'\U0001F1E6\U0001F1F7',
    'AS': u'\U0001F1E6\U0001F1F8',
    'AT': u'\U0001F1E6\U0001F1F9',
    'AU': u'\U0001F1E6\U0001F1FA',
    'AW': u'\U0001F1E6\U0001F1FC',
    'AX': u'\U0001F1E6\U0001F1FD',
    'AZ': u'\U0001F1E6\U0001F1FF',
    'BA': u'\U0001F1E7\U0001F1E6',
    'BB': u'\U0001F1E7\U0001F1E7',
    'BD': u'\U0001F1E7\U0001F1E9',
    'BE': u'\U0001F1E7\U0001F1EA',
    'BF': u'\U0001F1E7\U0001F1EB',
    'BG': u'\U0001F1E7\U0001F1EC',
    'BH': u'\U0001F1E7\U0001F1ED',
    'BI': u'\U0001F1E7\U0001F1EE',
    'BJ': u'\U0001F1E7\U0001F1EF',
    'BL': u'\U0001F1E7\U0001F1F1',
    'BM': u'\U0001F1E7\U0001F1F2',
    'BN': u'\U0001F1E7\U0001F1F3',
    'BO': u'\U0001F1E7\U0001F1F4',
    'BQ': u'\U0001F1E7\U0001F1F6',
    'BR': u'\U0001F1E7\U0001F1F7',
    'BS': u'\U0001F1E7\U0001F1F8',
    'BT': u'\U0001F1E7\U0001F1F9',
    'BV': u'\U0001F1E7\U0001F1FB',
    'BW': u'\U0001F1E7\U0001F1FC',
    'BY': u'\U0001F1E7\U0001F1FE',
    'BZ': u'\U0001F1E7\U0001F1FF',
    'CA': u'\U0001F1E8\U0001F1E6',
    'CC': u'\U0001F1E8\U0001F1E8',
    'CD': u'\U0001F1E8\U0001F1E9',
    'CF': u'\U0001F1E8\U0001F1EB',
    'CG': u'\U0001F1E8\U0001F1EC',
    'CH': u'\U0001F1E8\U0001F1ED',
    'CI': u'\U0001F1E8\U0001F1EE',
    'CK': u'\U0001F1E8\U0001F1F0',
    'CL': u'\U0001F1E8\U0001F1F1',
    'CM': u'\U0001F1E8\U0001F1F2',
    'CN': u'\U0001F1E8\U0001F1F3',
    'CO': u'\U0001F1E8\U0001F1F4',
    'CR': u'\U0001F1E8\U0001F1F7',
    'CU': u'\U0001F1E8\U0001F1FA',
    'CV': u'\U0001F1E8\U0001F1FB',
    'CW': u'\U0001F1E8\U0001F1FC',
    'CX': u'\U0001F1E8\U0001F1FD',
    'CY': u'\U0001F1E8\U0001F1FE',
    'CZ': u'\U0001F1E8\U0001F1FF',
    'DE': u'\U0001F1E9\U0001F1EA',
    'DJ': u'\U0001F1E9\U0001F1EF',
    'DK': u'\U0001F1E9\U0001F1F0',
    'DM': u'\U0001F1E9\U0001F1F2',
    'DO': u'\U0001F1E9\U0001F1F4',
    'DZ': u'\U0001F1E9\U0001F1FF',
    'EC': u'\U0001F1EA\U0001F1E8',
    'EE': u'\U0001F1EA\U0001F1EA',
    'EG': u'\U0001F1EA\U0001F1EC',
    'EH': u'\U0001F1EA\U0001F1ED',
    'ER': u'\U0001F1EA\U0001F1F7',
    'ES': u'\U0001F1EA\U0001F1F8',
    'ET': u'\U0001F1EA\U0001F1F9',
    'FI': u'\U0001F1EB\U0001F1EE',
    'FJ': u'\U0001F1EB\U0001F1EF',
    'FK': u'\U0001F1EB\U0001F1F0',
    'FM': u'\U0001F1EB\U0001F1F2',
    'FO': u'\U0001F1EB\U0001F1F4',
    'FR': u'\U0001F1EB\U0001F1F7',
    'GA': u'\U0001F1EC\U0001F1E6',
    'GB': u'\U0001F1EC\U0001F1E7',
    'GD': u'\U0001F1EC\U0001F1E9',
    'GE': u'\U0001F1EC\U0001F1EA',
    'GF': u'\U0001F1EC\U0001F1EB',
    'GG': u'\U0001F1EC\U0001F1EC',
    'GH': u'\U0001F1EC\U0001F1ED',
    'GI': u'\U0001F1EC\U0001F1EE',
    'GL': u'\U0001F1EC\U0001F1F1',
    'GM': u'\U0001F1EC\U0001F1F2',
    'GN': u'\U0001F1EC\U0001F1F3',
    'GP': u'\U0001F1EC\U0001F1F5',
    'GQ': u'\U0001F1EC\U0001F1F6',
    'GR': u'\U0001F1EC\U0001F1F7',
    'GS': u'\U0001F1EC\U0001F1F8',
    'GT': u'\U0001F1EC\U0001F1F9',
    'GU': u'\U0001F1EC\U0001F1FA',
    'GW': u'\U0001F1EC\U0001F1FC',
    'GY': u'\U0001F1EC\U0001F1FE',
    'HK': u'\U0001F1ED\U0001F1F0',
    'HM': u'\U0001F1ED\U0001F1F2',
    'HN': u'\U0001F1ED\U0001F1F3',
    'HR': u'\U0001F1ED\U0001F1F7',
    'HT': u'\U0001F1ED\U0001F1F9',
    'HU': u'\U0001F1ED\U0001F1FA',
    'ID': u'\U0001F1EE\U0001F1E9',
    'IE': u'\U0001F1EE\U0001F1EA',
    'IL': u'\U0001F1EE\U0001F1F1',
    'IM': u'\U0001F1EE\U0001F1F2',
    'IN': u'\U0001F1EE\U0001F1F3',
    'IO': u'\U0001F1EE\U0001F1F4',
    'IQ': u'\U0001F1EE\U0001F1F6',
    'IR': u'\U0001F1EE\U0001F1F7',
    'IS': u'\U0001F1EE\U0001F1F8',
    'IT': u'\U0001F1EE\U0001F1F9',
    'JE': u'\U0001F1EF\U0001F1EA',
    'JM': u'\U0001F1EF\U0001F1F2',
    'JO': u'\U0001F1EF\U0001F1F4',
    'JP': u'\U0001F1EF\U0001F1F5',
    'KE': u'\U0001F1F0\U0001F1EA',
    'KG': u'\U0001F1F0\U0001F1EC',
    'KH': u'\U0001F1F0\U0001F1ED',
    'KI': u'\U0001F1F0\U0001F1EE',
    'KM': u'\U0001F1F0\U0001F1F2',
    'KN': u'\U0001F1F0\U0001F1F3',
    'KP': u'\U0001F1F0\U0001F1F5',
    'KR': u'\U0001F1F0\U0001F1F7',
    'KW': u'\U0001F1F0\U0001F1FC',
    'KY': u'\U0001F1F0\U0001F1FE',
    'KZ': u'\U0001F1F0\U0001F1FF',
    'LA': u'\U0001F1F1\U0001F1E6',
    'LB': u'\U0001F1F1\U0001F1E7',
    'LC': u'\U0001F1F1\U0001F1E8',
    'LI': u'\U0001F1F1\U0001F1EE',
    'LK': u'\U0001F1F1\U0001F1F0',
    'LR': u'\U0001F1F1\U0001F1F7',
    'LS': u'\U0001F1F1\U0001F1F8',
    'LT': u'\U0001F1F1\U0001F1F9',
    'LU': u'\U0001F1F1\U0001F1FA',
    'LV': u'\U0001F1F1\U0001F1FB',
    'LY': u'\U0001F1F1\U0001F1FE',
    'MA': u'\U0001F1F2\U0001F1E6',
    'MC': u'\U0001F1F2\U0001F1E8',
    'MD': u'\U0001F1F2\U0001F1E9',
    'ME': u'\U0001F1F2\U0001F1EA',
    'MF': u'\U0001F1F2\U0001F1EB',
    'MG': u'\U0001F1F2\U0001F1EC',
    'MH': u'\U0001F1F2\U0001F1ED',
    'MK': u'\U0001F1F2\U0001F1F0',
    'ML': u'\U0001F1F2\U0001F1F1',
    'MM': u'\U0001F1F2\U0001F1F2',
    'MN': u'\U0001F1F2\U0001F1F3',
    'MO': u'\U0001F1F2\U0001F1F4',
    'MP': u'\U0001F1F2\U0001F1F5',
    'MQ': u'\U0001F1F2\U0001F1F6',
    'MR': u'\U0001F1F2\U0001F1F7',
    'MS': u'\U0001F1F2\U0001F1F8',
    'MT': u'\U0001F1F2\U0001F1F9',
    'MU': u'\U0001F1F2\U0001F1FA',
    'MV': u'\U0001F1F2\U0001F1FB',
    'MW': u'\U0001F1F2\U0001F1FC',
    'MX': u'\U0001F1F2\U0001F1FD',
    'MY': u'\U0001F1F2\U0001F1FE',
    'MZ': u'\U0001F1F2\U0001F1FF',
    'NA': u'\U0001F1F3\U0001F1E6',
    'NC': u'\U0001F1F3\U0001F1E8',
    'NE': u'\U0001F1F3\U0001F1EA',
    'NF': u'\U0001F1F3\U0001F1EB',
    'NG': u'\U0001F1F3\U0001F1EC',
    'NI': u'\U0001F1F3\U0001F1EE',
    'NL': u'\U0001F1F3\U0001F1F1',
    'NO': u'\U0001F1F3\U0001F1F4',
    'NP': u'\U0001F1F3\U0001F1F5',
    'NR': u'\U0001F1F3\U0001F1F7',
    'NU': u'\U0001F1F3\U0001F1FA',
    'NZ': u'\U0001F1F3\U0001F1FF',
    'OM': u'\U0001F1F4\U0001F1F2',
    'PA': u'\U0001F1F5\U0001F1E6',
    'PE': u'\U0001F1F5\U0001F1EA',
    'PF': u'\U0001F1F5\U0001F1EB',
    'PG': u'\U0001F1F5\U0001F1EC',
    'PH': u'\U0001F1F5\U0001F1ED',
    'PK': u'\U0001F1F5\U0001F1F0',
    'PL': u'\U0001F1F5\U0001F1F1',
    'PM': u'\U0001F1F5\U0001F1F2',
    'PN': u'\U0001F1F5\U0001F1F3',
    'PR': u'\U0001F1F5\U0001F1F7',
    'PS': u'\U0001F1F5\U0001F1F8',
    'PT': u'\U0001F1F5\U0001F1F9',
    'PW': u'\U0001F1F5\U0001F1FC',
    'PY': u'\U0001F1F5\U0001F1FE',
    'QA': u'\U0001F1F6\U0001F1E6',
    'RE': u'\U0001F1F7\U0001F1EA',
    'RO': u'\U0001F1F7\U0001F1F4',
    'RS': u'\U0001F1F7\U0001F1F8',
    'RU': u'\U0001F1F7\U0001F1FA',
    'RW': u'\U0001F1F7\U0001F1FC',
    'SA': u'\U0001F1F8\U0001F1E6',
    'SB': u'\U0001F1F8\U0001F1E7',
    'SC': u'\U0001F1F8\U0001F1E8',
    'SD': u'\U0001F1F8\U0001F1E9',
    'SE': u'\U0001F1F8\U0001F1EA',
    'SG': u'\U0001F1F8\U0001F1EC',
    'SH': u'\U0001F1F8\U0001F1ED',
    'SI': u'\U0001F1F8\U0001F1EE',
    'SJ': u'\U0001F1F8\U0001F1EF',
    'SK': u'\U0001F1F8\U0001F1F0',
    'SL': u'\U0001F1F8\U0001F1F1',
    'SM': u'\U0001F1F8\U0001F1F2',
    'SN': u'\U0001F1F8\U0001F1F3',
    'SO': u'\U0001F1F8\U0001F1F4',
    'SR': u'\U0001F1F8\U0001F1F7',
    'SS': u'\U0001F1F8\U0001F1F8',
    'ST': u'\U0001F1F8\U0001F1F9',
    'SV': u'\U0001F1F8\U0001F1FB',
    'SX': u'\U0001F1F8\U0001F1FD',
    'SY': u'\U0001F1F8\U0001F1FE',
    'SZ': u'\U0001F1F8\U0001F1FF',
    'TC': u'\U0001F1F9\U0001F1E8',
    'TD': u'\U0001F1F9\U0001F1E9',
    'TF': u'\U0001F1F9\U0001F1EB',
    'TG': u'\U0001F1F9\U0001F1EC',
    'TH': u'\U0001F1F9\U0001F1ED',
    'TJ': u'\U0001F1F9\U0001F1EF',
    'TK': u'\U0001F1F9\U0001F1F0',
    'TL': u'\U0001F1F9\U0001F1F1',
    'TM': u'\U0001F1F9\U0001F1F2',
    'TN': u'\U0001F1F9\U0001F1F3',
    'TO': u'\U0001F1F9\U0001F1F4',
    'TR': u'\U0001F1F9\U0001F1F7',
    'TT': u'\U0001F1F9\U0001F1F9',
    'TV': u'\U0001F1F9\U0001F1FB',
    'TW': u'\U0001F1F9\U0001F1FC',
    'TZ': u'\U0001F1F9\U0001F1FF',
    'UA': u'\U0001F1FA\U0001F1E6',
    'UG': u'\U0001F1FA\U0001F1EC',
    'UM': u'\U0001F1FA\U0001F1F2',
    'US': u'\U0001F1FA\U0001F1F8',
    'UY': u'\U0001F1FA\U0001F1FE',
    'UZ': u'\U0001F1FA\U0001F1FF',
    'VA': u'\U0001F1FB\U0001F1E6',
    'VC': u'\U0001F1FB\U0001F1E8',
    'VE': u'\U0001F1FB\U0001F1EA',
    'VG': u'\U0001F1FB\U0001F1EC',
    'VI': u'\U0001F1FB\U0001F1EE',
    'VN': u'\U0001F1FB\U0001F1F3',
    'VU': u'\U0001F1FB\U0001F1FA',
    'WF': u'\U0001F1FC\U0001F1EB',
    'WS': u'\U0001F1FC\U0001F1F8',
    'YE': u'\U0001F1FE\U0001F1EA',
    'YT': u'\U0001F1FE\U0001F1F9',
    'ZA': u'\U0001F1FF\U0001F1E6',
    'ZM': u'\U0001F1FF\U0001F1F2',
    'ZW': u'\U0001F1FF\U0001F1FC',
}


def get_address_url(point):
    return GEOCODE_URL.format(latlng=point, api_key=config.GOOGLE_MAPS_API_KEY)


def get_time_zone_url(point):
    return TIME_ZONE_URL.format(latlng=point,
                                timestamp=int(time.time()),
                                api_key=config.GOOGLE_MAPS_API_KEY)


def get_weather_url(point):
    return WEATHER_URL.format(latlng=point,
                              api_key=config.FORECASTIO_API_KEY)


class LocationInfo(ndb.Model):
    city = ndb.StringProperty()
    country = ndb.StringProperty()
    location = ndb.GeoPtProperty()
    timestamp = ndb.DateTimeProperty(auto_now_add=True, default=datetime.min)
    timezone = ndb.StringProperty()

    def __eq__(self, other):
        if isinstance(other, ndb.GeoPt):
            other_point = other
        elif isinstance(other, LocationInfo):
            other_point = other.location
        else:
            return False
        return self.location == other_point

    def __ne__(self, other):
        return not self.__eq__(other)

    def distance_km(self, other):
        a = self.location
        if isinstance(other, ndb.GeoPt):
            b = other
        elif isinstance(other, LocationInfo):
            b = other.location
        else:
            raise TypeError('Expected GeoPt or LocationInfo')
        degrees_to_radians = math.pi / 180
        phi_a = (90 - a.lat) * degrees_to_radians
        phi_b = (90 - b.lat) * degrees_to_radians
        theta_a = a.lon * degrees_to_radians
        theta_b = b.lon * degrees_to_radians
        cos = (math.sin(phi_a) * math.sin(phi_b) * math.cos(theta_a - theta_b) +
               math.cos(phi_a) * math.cos(phi_b))
        arc = math.acos(cos)
        return arc * 6373  # km

    @classmethod
    def from_point(cls, point, timezone_only=False):
        # TODO: Use timezone only flag.
        # Simplify the point to three decimals.
        point = ndb.GeoPt(round(point.lat, 3), round(point.lon, 3))
        if not point.lat and not point.lon:
            # Don't attempt to look up (0, 0).
            return None
        # Fetch the necessary data from APIs.
        address_future = api_request(get_address_url(point))
        weather_future = api_request(get_weather_url(point), cache_duration=3600)
        # Ensure that both requests succeeded.
        try:
            address_data = address_future()
            assert address_data['status'] == 'OK', 'API error'
            assert address_data is not None, 'No address data'
            assert len(address_data['results']) > 0, 'No address results'
            weather_data = weather_future()
            assert weather_data is not None, 'No weather data'
        except Exception as e:
            logging.warning('Failed to get location info: {}'.format(e))
            return None
        # Extract relevant fields from the address data.
        a = defaultdict(list)
        for result in address_data['results']:
            for component in result['address_components']:
                value = component['long_name']
                for key in component['types']:
                    a[key].append(value)
                    if key == 'country':
                        a['country_code'].append(component['short_name'])
        logging.debug('Locations:\n%s', a)
        # Create a LocationInfo object.
        info = cls(location=point)
        # TODO: If the point is more accurate, we could use neighborhood or sublocality here.
        info.country = a['country_code'][0]
        info.city = (a['locality'] + a['administrative_area_level_1'] + a['country'])[0]
        info.timezone = weather_data['timezone']
        return info

    def get_weather(self):
        return self.get_weather_async()()

    def get_weather_async(self):
        if not self.location:
            return lambda: None
        future = api_request(get_weather_url(self.location), cache_duration=3600)
        def get_result():
            try:
                return Weather(future())
            except Exception as e:
                logging.warning('Failed to get weather data: %s', e)
                return None
        return get_result


class Weather(object):
    def __init__(self, data):
        if not data or 'currently' not in data:
            self._has_data = False
            return
        self._has_data = True
        currently = data['currently']
        # Set a few weather properties that we care about.
        self.cloudiness = currently['cloudCover']
        self.precipitation = currently['precipIntensity']
        self.phenomenon = re.sub('-(day|night)$', '', currently['icon'])
        self.temperature = currently['temperature']
        self.wind = currently['windSpeed']
        daily = data['daily']['data'][0]
        self.temperature_high = daily['temperatureMax']
        self.temperature_low = daily['temperatureMin']
        # If possible, refresh values with forecast data.
        if 'minutely' in data:
            minutely = data['minutely']
            now = time.time()
            diff = abs(now - currently['time'])
            for point in minutely['data']:
                if point['precipIntensity'] and point['precipProbability'] < 0.2:
                    continue
                test_diff = abs(now - point['time'])
                if test_diff > diff:
                    continue
                diff = test_diff
                break
            else:
                return
            self.precipitation = point['precipIntensity']
            if point.get('precipType') in ('rain', 'sleet', 'snow'):
                self.phenomenon = point['precipType']
            elif not self.precipitation and self.phenomenon in ('rain', 'sleet', 'snow'):
                # TODO: We guess that it's cloudy if it was just precipitating, fix this.
                self.phenomenon = 'cloudy'

    def public(self, version=None, **kwargs):
        if not self._has_data:
            return None
        if version < 10:
            return self.phenomenon
        return {
            'cloudiness': self.cloudiness,
            'precipitation': self.precipitation,
            'temperature': self.temperature,
            'temperature_high': self.temperature_high,
            'temperature_low': self.temperature_low,
            'weather': self.phenomenon,
            'wind': self.wind,
        }
