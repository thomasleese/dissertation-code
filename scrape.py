import sys
import time

import geopy.exc
from geopy.geocoders import GoogleV3
from joblib import Memory
import pymysql
import requests

from dataset import Database, Events
import settings


memory = Memory('cache/scrape', verbose=0)


class RateLimitError(RuntimeError):
    def __init__(self, reset_time):
        self.reset_time = reset_time

    @property
    def seconds_left(self):
        return self.reset_time - int(time.time())

    @property
    def finished(self):
        return self.seconds_left <= 0

    def wait(self):
        if self.finished:
            return

        seconds = self.seconds_left
        minutes = round(self.seconds_left / 60, 1)
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
            raise RateLimitError(reset_time)

        return response

    def get_all_users(self, since=0):
        next_url = 'https://api.github.com/users?since={}'.format(since)

        while next_url is not None:
            response = self.get(next_url)
            next_url = response.links['next']['url']
            for user in response.json():
                yield user

    def get_user(self, username):
        url = 'https://api.github.com/users/{}'.format(username)
        return self.get(url).json()

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
            raise RateLimitError(int(time.time()) + (60 * 60))
        except geopy.exc.GeocoderTimedOut:
            raise RateLimitError(int(time.time()) + 10)
        except geopy.exc.GeocoderServiceError:
            raise RateLimitError(int(time.time()) + 10)
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
            raise RateLimitError(int(time.time()) + (60 * 60))

        if headers['X-Rate-Limit-Remaining'] == '0':
            reset_time = int(headers['X-Rate-Limit-Reset'])
            raise RateLimitError(reset_time)

        data = response.json()

        if data['gender'] is None:
            return '?', None

        probability = float(data['probability'])
        code = data['gender'][0].upper()
        return code, probability


class Scraper:
    def __init__(self):
        self.github = GitHub()
        self.genderize = Genderize()
        self.geography = Geography()

        self.database = Database()
        self.events = Events()

    def print_status(self, user_id, *args):
        print('>', '#{}'.format(user_id), *args)
        sys.stdout.flush()

    def scrape_event(self, event):
        try:
            actor = event['actor']
            user_id = actor['id']
            user_login = actor['login']
        except TypeError:
            user_login = event['actor']
            user_id = None
        except KeyError:
            return

        if self.database.has_user(user_login, user_id):
            return

        github_user = self.github.get_user(user_login)
        if user_id is not None and ('id' not in github_user or user_id != github_user['id']):  # no longer a user
            self.database.insert_user({'id': user_id, 'login': user_login, 'deleted': True})
            self.print_status(user_id, ':(')
            return

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

        try:
            self.database.insert_user(fields)
        except pymysql.err.IntegrityError:
            self.print_status(user_id, user_login, ':(')
        else:
            self.print_status(fields['id'], fields['login'], github_user['created_at'], '✓')

    def scrape(self, start_from):
        for event in self.events.iterate(start_from=start_from):
            while True:
                try:
                    self.scrape_event(event)
                    break
                except RateLimitError as e:
                    while not e.finished:
                        try:
                            self.scrape_locations(25)
                            self.scrape_genders(25)
                        except RateLimitError:
                            break

                    e.wait()

    def scrape_locations(self, n=100):
        users = self.database.get_users_without_location(limit=n)
        for user_id, location_str in users:
            location = self.geography.geocode(location_str)
            if location is None:
                self.print_status(user_id, location_str, '->', '?')
                self.database.update_user_location(user_id, None, None, '?')
                continue

            try:
                country_code, country_name = \
                    self.geography.get_country(location)
            except ValueError:
                self.print_status(user_id, location_str, '->', '?')
                self.database.update_user_location(user_id, None, None, '?')
                continue

            self.print_status(user_id, location_str, '->', country_code,
                              '({})'.format(country_name))

            self.database.update_user_location(user_id, location.latitude,
                                               location.longitude,
                                               country_code)

    def scrape_genders(self, n=100):
        users = self.database.get_users_without_gender(limit=n)
        for user_id, name in users:
            gender, probability = self.genderize.guess(name.split()[0])

            try:
                probability_str = '({}%)'.format(probability * 100)
            except TypeError:
                probability_str = '(N/A)'

            self.print_status(user_id, name, '->', gender, probability_str)

            self.database.update_user_gender(user_id, gender, probability)


if __name__ == '__main__':
    scraper = Scraper()
    scraper.scrape(sys.argv[1])
