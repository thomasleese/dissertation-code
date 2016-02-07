import sys
import time

import geopy.exc
from geopy.geocoders import GoogleV3
from joblib import Memory
import requests

from dataset import Database
import settings


memory = Memory('cache/scrape', verbose=0)


def rate_limit_sleep(seconds):
    minutes = round(seconds / 60, 1)
    print('!', 'Waiting', minutes, 'minutes.')
    time.sleep(seconds)


class GitHub:
    def __init__(self):
        self.client_id = settings.CLIENT_ID
        self.client_secret = settings.CLIENT_SECRET

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
        self.api_key = settings.GOOGLE_API_KEY
        self.geolocator = GoogleV3(self.api_key)

        self.geocode = memory.cache(self.geocode)

    def geocode(self, text):
        try:
            result = self.geolocator.geocode(text)
        except geopy.exc.GeocoderQuotaExceeded:
            rate_limit_sleep(60 * 60)
            return self.geocode(text)
        except geopy.exc.GeocoderTimedOut:
            rate_limit_sleep(10)
            return self.geocode(text)
        else:
            time.sleep(0.1)  # to avoid rate limiting
            return result

    @staticmethod
    def get_country(location):
        for component in location.raw['address_components']:
            if 'country' in component['types']:
                return component['short_name'], component['long_name']

        raise ValueError()


class Genderize:
    def __init__(self):
        self.api_key = settings.GENDERIZE_API_KEY
        self.guess = memory.cache(self.guess)

    def guess(self, name):
        if name == "<script>alert('test')</script>":
            return '?', None

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


def print_status(user_id, *args):
    print('>', '#{}'.format(user_id), *args)
    sys.stdout.flush()


def users():
    github = GitHub()
    database = Database()

    last_user_id = database.last_user_id

    users = github.get_all_users(last_user_id)
    for github_user in users:
        github_user = github.get(github_user['url']).json()
        if 'id' not in github_user:
            continue  # not a user

        fields = {
            'id': github_user['id'],
            'hireable': github_user['hireable']
        }

        for field in ['login', 'avatar_url', 'gravatar_id', 'name', 'company',
                      'blog', 'location', 'email', 'bio']:
            if github_user[field] is None:
                fields[field] = None
            else:
                fields[field] = github_user[field].strip()

        database.insert_user(fields)

        print_status(fields['id'], fields['login'], '✓')

    database.close()


def user_followings():
    github = GitHub()

    """for user in session.query(User):
        for other_user in github.get_following_users(user.login):
            print(other_user)"""


def user_locations():
    geography = Geography()
    database = Database()

    for user_id, location_str in database.users_without_location:
        location = geography.geocode(location_str)
        if location is None:
            continue

        try:
            country_code, country_name = geography.get_country(location)
        except ValueError:
            continue

        print_status(user_id, location_str, '->', country_code,
                     '({})'.format(country_name))

        database.update_user_location(user_id, location.latitude,
                                      location.longitude, country_code)


def user_genders():
    genderize = Genderize()
    database = Database()

    for user_id, name in database.users_without_gender:
        gender, probability = genderize.guess(name.split()[0])

        try:
            probability_str = '({}%)'.format(probability * 100)
        except TypeError:
            probability_str = '(N/A)'

        print_status(user_id, name, '->', gender, probability_str)

        database.update_user_gender(user_id, gender, probability)


if __name__ == '__main__':
    for name in sys.argv[1:]:
        locals()[name]()
