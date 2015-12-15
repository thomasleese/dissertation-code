import os
import time

import requests


CLIENT_ID = os.environ['CLIENT_ID']
CLIENT_SECRET = os.environ['CLIENT_SECRET']


def get(url, params=None):
    print('>', url)

    if params is None:
        params = {}

    params['client_id'] = CLIENT_ID
    params['client_secret'] = CLIENT_SECRET

    response = requests.get(url, params=params)

    headers = response.headers
    if headers['X-RateLimit-Remaining'] == '0':
        reset_time = int(headers['X-RateLimit-Reset'])
        seconds_remaining = reset_time - int(time.time())
        print('!', 'Waiting', seconds_remaining, 'seconds.')
        time.sleep(seconds_remaining)
        return get(url, params)

    return response


def get_all_users(since=0):
    next_url = 'https://api.github.com/users?since={}'.format(since)

    while next_url is not None:
        response = get(next_url)
        next_url = response.links['next']['url']
        for user in response.json():
            yield user
