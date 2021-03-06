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
        response = requests.get('https://api.genderize.io', params=params)

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

    def scrape_user_details(self, start_from):
        i = 0

        for event in self.events.iterate(start_from=start_from):
            try:
                actor = event['actor']
                login = actor['login']
            except TypeError:
                login = event['actor']
            except KeyError:
                continue

            if login is None:
                continue

            if 'actor_attributes' not in event:
                continue

            github_user = event['actor_attributes']
            github_user.setdefault('id', None)
            github_user.setdefault('name', None)
            github_user.setdefault('hireable', None)
            github_user.setdefault('company', None)
            github_user.setdefault('blog', None)
            github_user.setdefault('location', None)
            github_user.setdefault('bio', None)

            fields = {
                'id': github_user['id'],
                'hireable': github_user['hireable'],
                'deleted': False,
            }

            for field in ['name', 'company', 'blog', 'location', 'bio']:
                if github_user[field] is None:
                    fields[field] = None
                else:
                    fields[field] = github_user[field].strip()

            self.database.update_user(login, fields)

            i += 1

            if i > 100:
                self.database.commit()
                i = 0

        self.database.commit()

    def scrape_user_logins(self, start_from):
        logins = set()

        for event in self.events.iterate(start_from=start_from):
            try:
                actor = event['actor']
                login = actor['login']
            except TypeError:
                login = event['actor']
            except KeyError:
                continue

            if login is None:
                continue

            logins.add(login)

            if len(logins) >= 100000:
                self.database.insert_many_users(logins)
                self.database.commit()
                logins = set()

        self.database.insert_many_users(logins)
        self.database.commit()

    def scrape_user_activity(self, start_from):
        first_active = {}
        last_active = {}

        for event in self.events.iterate(start_from=start_from):
            try:
                actor = event['actor']
                login = actor['login']
            except TypeError:
                login = event['actor']
            except KeyError:
                continue

            if login is None:
                continue

            active_date = event['created_at']

            if login not in first_active:
                first_active[login] = active_date
            last_active[login] = active_date

            if len(last_active) >= 100000:
                self.database.update_user_activity(first_active, last_active)
                first_active = {}
                last_active = {}

        self.database.update_user_activity(first_active, last_active)

    def scrape_user_events(self, start_from):
        i = 0

        for event in self.events.iterate(start_from=start_from):
            try:
                actor = event['actor']
                login = actor['login']
            except TypeError:
                login = event['actor']
            except KeyError:
                continue

            if login is None:
                continue

            self.database.add_user_event(login, event['type'])

            i += 1
            if i >= 5000:
                self.database.commit()
                i = 0

        self.database.commit()

    def scrape_locations(self):
        locations = {}

        users = self.database.get_users_without_location()
        len_users = len(users)

        for i, (login, location_str) in enumerate(users):
            location = self.geography.geocode(location_str)
            if location is None:
                print(login, location_str, '->', '?')
                locations[login] = (None, None, '?')
                continue

            try:
                country_code, country_name = \
                    self.geography.get_country(location)
            except ValueError:
                print(login, location_str, '->', '?')
                locations[login] = (None, None, '?')
                continue

            progress = '{:<8}%'.format(round((i / len_users) * 100, 2))

            print(progress, location_str, '->', country_code,
                  '({})'.format(country_name))

            locations[login] = (location.latitude, location.longitude,
                                country_code)

            if len(locations) > 1000:
                self.database.update_user_location(locations)
                locations = {}

        self.database.update_user_location(locations)

        print('Finished.')

    def scrape_genders(self):
        genders = {}

        users = self.database.get_users_without_gender()
        len_users = len(users)

        for i, (login, name) in enumerate(users):
            gender, probability = self.genderize.guess(name.split()[0])

            try:
                probability_str = '({}%)'.format(probability * 100)
            except TypeError:
                probability_str = '(N/A)'

            progress = '{:<8}%'.format(round((i / len_users) * 100, 2))

            print(progress, name, '->', gender, probability_str)

            genders[login] = (gender, probability)

            if len(genders) >= 1000:
                self.database.update_user_gender(genders)
                genders = {}

        self.database.update_user_gender(genders)

        print('Finished.')

    def scrape_project_names(self, start_from):
        names = set()

        for event in self.events.iterate(start_from=start_from):
            try:
                repository = event['repository']
            except KeyError:
                continue

            names.add((repository['owner'], repository['name']))

            if len(names) >= 100000:
                self.database.insert_many_repositories(names)
                self.database.commit()
                names = set()

        self.database.insert_many_repositories(names)
        self.database.commit()

    def scrape_project_details(self, start_from):
        i = 0

        for event in self.events.iterate(start_from=start_from):
            try:
                repository = event['repository']
            except KeyError:
                continue

            fields = {
                'is_fork': repository['fork']
            }

            for field in ['language', 'stargazers', 'has_downloads', 'has_issues', 'watchers', 'open_issues', 'size', 'has_wiki', 'forks']:
                fields[field] = repository.get(field, None)

            self.database.update_project(repository['owner'], repository['name'], fields)

            i += 1

            if i > 100:
                self.database.commit()
                i = 0

        self.database.commit()


def scrape(scraper):
    print('Running scraper...')

    if sys.argv[1] == 'user_details':
        scraper.scrape_user_details(sys.argv[2])
    elif sys.argv[1] == 'user_logins':
        scraper.scrape_user_logins(sys.argv[2])
    elif sys.argv[1] == 'user_activity':
        scraper.scrape_user_activity(sys.argv[2])
    elif sys.argv[1] == 'user_events':
        scraper.scrape_user_events(sys.argv[2])
    elif sys.argv[1] == 'genders':
        scraper.scrape_genders()
    elif sys.argv[1] == 'locations':
        scraper.scrape_locations()
    elif sys.argv[1] == 'project_names':
        scraper.scrape_project_names(sys.argv[2])
    elif sys.argv[1] == 'project_details':
        scraper.scrape_project_details(sys.argv[2])
    else:
        raise RuntimeError(sys.argv[1])

    print('Scraper claims to have finished successfully.')


if __name__ == '__main__':
    scraper = Scraper()

    finished = False

    while not finished:
        try:
            scrape(scraper)
        except RateLimitError as e:
            print('Rate limit error!')
            e.wait()
        else:
            finished = True
