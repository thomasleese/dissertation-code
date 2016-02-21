import sys
import time
import warnings

import geopy.exc
from geopy.geocoders import GoogleV3
from joblib import Memory
import pymysql
import requests

from dataset import Database, Events
import settings


warnings.filterwarnings('ignore', category=pymysql.Warning)

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
            login = actor['login']
        except TypeError:
            login = event['actor']
        except KeyError:
            return

        if actor is None:
            return

        if 'actor_attributes' in event:
            github_user = event['actor_attributes']
            github_user.setdefault('id', None)
            github_user.setdefault('name', None)
            github_user.setdefault('hireable', None)
            github_user.setdefault('company', None)
            github_user.setdefault('blog', None)
            github_user.setdefault('location', None)
            github_user.setdefault('bio', None)
        else:
            if self.database.has_user(login):
                return

            print('From GitHub:', login)

            github_user = self.github.get_user(login)

            if 'id' not in github_user:
                self.database.insert_user({'login': login, 'deleted': True})
                self.print_status(login, ':(')
                return

        fields = {
            'id': github_user['id'],
            'hireable': github_user['hireable'],
            'deleted': False
        }

        for field in ['login', 'name', 'company', 'blog', 'location', 'bio']:
            if github_user[field] is None:
                fields[field] = None
            else:
                fields[field] = github_user[field].strip()

        self.database.insert_user(fields)
        #self.print_status(fields['login'], '✓')

    def scrape_users(self, start_from):
        for event in self.events.iterate(start_from=start_from):
            self.scrape_event(event)

    def scrape_locations(self):
        users = self.database.get_users_without_location()
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

    def scrape_genders(self):
        users = self.database.get_users_without_gender()
        for user_id, name in users:
            gender, probability = self.genderize.guess(name.split()[0])

            try:
                probability_str = '({}%)'.format(probability * 100)
            except TypeError:
                probability_str = '(N/A)'

            self.print_status(user_id, name, '->', gender, probability_str)

            self.database.update_user_gender(user_id, gender, probability)


def scrape(scraper):
    if sys.argv[1] == 'users':
        scraper.scrape_users(sys.argv[2])
    elif sys.argv[1] == 'genders':
        scraper.scrape_genders()
    elif sys.argv[1] == 'locations':
        scraper.scrape_locations()


if __name__ == '__main__':
    scraper = Scraper()

    while True:
        try:
            scrape(scraper)
        except RateLimitError as e:
            e.wait()
