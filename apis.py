import os
import time

from geopy.geocoders import GoogleV3
import requests


class GitHub:
    def __init__(self):
        self.client_id = os.environ['CLIENT_ID']
        self.client_secret = os.environ['CLIENT_SECRET']

    def get(self, url, params=None):
        print('>', url)

        if params is None:
            params = {}

        params['client_id'] = self.client_id
        params['client_secret'] = self.client_secret

        response = requests.get(url, params=params)

        headers = response.headers
        if headers['X-RateLimit-Remaining'] == '0':
            reset_time = int(headers['X-RateLimit-Reset'])
            seconds_remaining = reset_time - int(time.time())
            print('!', 'Waiting', seconds_remaining, 'seconds.')
            time.sleep(seconds_remaining)
            return self.get(url, params)

        return response

    def get_all_users(self, since=0):
        next_url = 'https://api.github.com/users?since={}'.format(since)

        while next_url is not None:
            response = self.get(next_url)
            next_url = response.links['next']['url']
            for user in response.json():
                yield user


class Geography:
    def __init__(self):
        self.api_key = os.environ['GOOGLE_API_KEY']
        self.geolocator = GoogleV3(self.api_key)

    def get_country(self, text):
        time.sleep(1)  # to avoid rate limite

        location = self.geolocator.geocode(text)
        if location is None:
            raise ValueError()

        for component in location.raw['address_components']:
            if 'country' in component['types']:
                return component['short_name'], component['long_name']

        raise ValueError()


class Genderize:
    def guess(self, name):
        time.sleep(1)  # to avoid rate limite

        params = {'name': name}
        response = requests.get('http://api.genderize.io', params=params)
        data = response.json()

        if data['gender'] is None:
            raise ValueError()

        probability = float(data['probability'])
        if probability >= 0.8:
            return data['gender'][0].upper(), probability
        else:
            raise ValueError()
