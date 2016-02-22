import gc
import gzip
import json
from collections import Counter, OrderedDict
from pathlib import Path
import sys

import pymysql
from joblib import Memory

import settings

memory = Memory('cache/dataset', verbose=0)


class Database:
    def __init__(self):
        self.connection = pymysql.connect(host=settings.DB_HOST,
                                          user=settings.DB_USER,
                                          password=settings.DB_PASSWORD,
                                          db=settings.DB_NAME,
                                          charset='utf8')

    def cursor(self):
        return self.connection.cursor()

    def commit(self):
        self.connection.commit()

    def close(self):
        self.connection.close()

    def count(self):
        counter = Counter()
        with self.cursor() as cursor:
            cursor.execute('SELECT COUNT(id) FROM users')
            counter['users'] = cursor.fetchone()[0]

            cursor.execute('SELECT COUNT(DISTINCT company) FROM users')
            counter['companies'] = cursor.fetchone()[0]

            cursor.execute('SELECT COUNT(DISTINCT location_country) FROM users')
            counter['countries'] = cursor.fetchone()[0]

        return counter

    @property
    def last_user_id(self):
        with self.cursor() as cursor:
            cursor.execute('SELECT id FROM users ORDER BY id DESC LIMIT 1')
            return cursor.fetchone()[0]

    def has_user(self, user_login):
        sql = 'SELECT COUNT(*) FROM users WHERE login = %s'
        with self.cursor() as cursor:
            cursor.execute(sql, (user_login,))
            return cursor.fetchone()[0] > 0

    def insert_user(self, fields):
        for key in list(fields.keys()):
            if fields[key] is None:
                del fields[key]

        keys = list(fields.keys())

        with self.cursor() as cursor:
            fields_str = ', '.join(keys)
            values_str = ', '.join(['%s'] * len(fields))
            update_str = ', '.join('{} = %s'.format(k) for k in keys)
            sql = 'INSERT INTO users ({}) VALUES ({}) ON DUPLICATE KEY UPDATE {}' \
                .format(fields_str, values_str, update_str)
            cursor.execute(sql, tuple(fields.values()) * 2)

        self.commit()

    def update_user_first_active(self, login, first_active):
        with self.cursor() as cursor:
            cursor.execute("""
                UPDATE users
                SET first_active = %s
                WHERE login = %s and first_active > %s
            """, (first_active, login, first_active))

        self.commit()

    def get_company_distribution(self):
        with self.cursor() as cursor:
            cursor.execute("""
                SELECT company, COUNT(*)
                FROM users
                WHERE company IS NOT NULL
                GROUP BY company
            """)
            return OrderedDict(cursor)

    def get_country_distribution(self):
        with self.cursor() as cursor:
            cursor.execute("""
                SELECT location_country, COUNT(id)
                FROM users
                WHERE location_country IS NOT NULL
                GROUP BY location_country
            """)
            return OrderedDict(cursor)

    def get_gender_distribution(self):
        with self.cursor() as cursor:
            cursor.execute("""
                SELECT gender, COUNT(id)
                FROM users
                WHERE gender IS NOT NULL
                GROUP BY gender
            """)
            return OrderedDict(cursor)

    def get_location_points(self):
        with self.cursor() as cursor:
            cursor.execute("""
                SELECT location_latitude, location_longitude
                FROM users
                WHERE location_latitude IS NOT NULL
                    AND location_longitude IS NOT NULL
            """)

            for row in cursor:
                yield row

    def get_users_without_location(self):
        with self.cursor() as cursor:
            cursor.execute("""
                SELECT login, location
                FROM users
                WHERE location IS NOT NULL
                    AND location_country IS NULL
                    AND location_latitude IS NULL
                    AND location_longitude IS NULL
            """)

            for row in cursor:
                yield row

    def get_users_without_gender(self):
        with self.cursor() as cursor:
            cursor.execute("""
                SELECT login, name
                FROM users
                WHERE name IS NOT NULL
                    AND name != ''
                    AND (gender IS NULL OR gender != '?')
                    AND gender_probability IS NULL
            """)

            for row in cursor:
                yield row

    def update_user_gender(self, user_id, gender, probability):
        with self.cursor() as cursor:
            args = (gender, probability, user_id)
            cursor.execute("""
                UPDATE users
                SET gender = %s, gender_probability = %s
                WHERE login = %s
            """, args)

            self.commit()

    def update_user_location(self, user_id, latitude, longitude, country_code):
        with self.cursor() as cursor:
            args = (country_code, latitude, longitude, user_id)
            cursor.execute("""
                UPDATE users
                SET location_country = %s,
                    location_latitude = %s,
                    location_longitude = %s
                WHERE login = %s
            """, args)

        self.commit()


class Events:
    def __init__(self):
        self.path = Path('../data')

        self.count = memory.cache(self.count)
        self.count_types = memory.cache(self.count_types)

    def iterate(self, glob='*.json.gz', func=None, start_from=None):
        gc.disable()

        started = start_from is None

        for path in self.path.glob(glob):
            if not started:
                if path.name.startswith(start_from):
                    started = True
                else:
                    print('Skipping events:', path)
                    continue

            print('Loading events:', path)
            with gzip.open(str(path), 'rt', errors='ignore') as file:
                for line in file:
                    try:
                        record = json.loads(line)
                    except ValueError:
                        continue

                    if record['type'] == 'Event':
                        continue

                    if func is not None:
                        record = func(record)

                    yield record

    def count(self):
        iterator = self.iterate(func=lambda event: event['type'])
        counter = Counter(iterator)
        return counter

    @property
    def types(self):
        return list(self.count().keys())

    def count_types(self, year, month):
        glob = '{}-{:02d}-*.json.gz'.format(year, month)
        iterator = self.iterate(glob, func=lambda e: e['type'])
        return Counter(iterator)


def count():
    db = Database()
    events = Events()

    print(db.count())
    print(events.count())

    db.close()


def iterate_events():
    events = Events()
    for event in events.iterate():
        print(event)
        input()


if __name__ == '__main__':
    if sys.argv[1] == 'count':
        count()
    elif sys.argv[1] == 'events':
        iterate_events()
