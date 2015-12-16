import os
import time

import geopy.exc
from geopy.geocoders import GoogleV3
import requests


def rate_limit_sleep(seconds):
    minutes = round(seconds / 60, 1)
    print('!', 'Waiting', minutes, 'minutes.')
    time.sleep(seconds)


class GitHub:
    def __init__(self):
        self.client_id = os.environ['CLIENT_ID']
        self.client_secret = os.environ['CLIENT_SECRET']

    def get(self, url, params=None):
        if params is None:
            params = {}

        params['client_id'] = self.client_id
        params['client_secret'] = self.client_secret

        response = requests.get(url, params=params)

        headers = response.headers
        if headers['X-RateLimit-Remaining'] == '0':
            reset_time = int(headers['X-RateLimit-Reset'])
            seconds_remaining = reset_time - int(time.time())
            rate_limit_sleep(seconds_remaining)
            return self.get(url, params)

        return response

    def get_all_users(self, since=0):
        next_url = 'https://api.github.com/users?since={}'.format(since)

        while next_url is not None:
            response = self.get(next_url)
            next_url = response.links['next']['url']
            for user in response.json():
                yield user

    def get_following_users(self, username):
        next_url = 'https://api.github.com/users/{}/following'.format(username)

        while next_url is not None:
            response = self.get(next_url)

            try:
                next_url = response.links['next']['url']
            except KeyError:
                next_url = None

            for user in response.json():
                yield user


class Geography:
    def __init__(self):
        self.api_key = os.environ['GOOGLE_API_KEY']
        self.geolocator = GoogleV3(self.api_key)

    def geocode(self, text):
        try:
            return self.geolocator.geocode(text)
        except geopy.exc.GeocoderQuotaExceeded:
            rate_limit_sleep(60 * 60)
            return self.geocode(text)

    def get_country(self, location):
        for component in location.raw['address_components']:
            if 'country' in component['types']:
                return component['short_name'], component['long_name']

        raise ValueError()


class Genderize:
    def __init__(self):
        self.api_key = os.environ['GENDERIZE_API_KEY']

    def guess(self, name):
        params = {'name': name, 'apikey': self.api_key}
        response = requests.get('http://api.genderize.io', params=params)

        headers = response.headers
        if 'X-Rate-Limit-Remaining' not in headers:
            rate_limit_sleep(60 * 60)
            return self.guess(name)

        if headers['X-Rate-Limit-Remaining'] == '0':
            reset_time = int(headers['X-Rate-Limit-Reset'])
            seconds_remaining = reset_time
            rate_limit_sleep(seconds_remaining)
            return self.guess(name)

        data = response.json()

        if data['gender'] is None:
            return '?', None

        probability = float(data['probability'])
        code = data['gender'][0].upper()
        return code, probability
