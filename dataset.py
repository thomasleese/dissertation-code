from collections import Counter, OrderedDict
import gc
import gzip
import json
import os
from pathlib import Path

import msgpack
import pymysql


class Database:
    def __init__(self):
        self.connection = pymysql.connect(host=os.environ['DB_HOST'],
                                          user=os.environ['DB_USER'],
                                          password=os.environ['DB_PASSWORD'],
                                          db=os .environ['DB_NAME'],
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

    def insert_user(self, fields):
        with self.cursor() as cursor:
            fields_str = ', '.join(fields.keys())
            values_str = ', '.join(['%s'] * len(fields))
            sql = 'INSERT INTO users ({}) VALUES ({})' \
                .format(fields_str, values_str)
            cursor.execute(sql, tuple(fields.values()))

        self.commit()

    def get_company_distribution(self):
        with self.cursor() as cursor:
            cursor.execute("""
                SELECT company, COUNT(id)
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

    @property
    def users_without_location(self):
        with self.cursor() as cursor:
            cursor.execute("""
                SELECT id, location
                FROM users
                WHERE location IS NOT NULL
                    AND (location_country IS NULL
                        OR location_latitude IS NULL
                        OR location_longitude IS NULL)
            """)

            for row in cursor:
                yield row

    @property
    def users_without_gender(self):
        with self.cursor() as cursor:
            cursor.execute("""
                SELECT id, name
                FROM users
                WHERE name IS NOT NULL
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
                WHERE id = %s
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
                WHERE id = %s
            """, args)

        self.commit()


class Events:
    def __init__(self):
        self.path = Path('../data')

    def iterate_json(self, func=None):
        gc.disable()
        for path in self.path.iterdir():
            with gzip.open(str(path), 'rt') as file:
                for line in file:
                    record = json.loads(line)
                    if func is None:
                        yield record
                    else:
                        yield func(record)

    def iterate_msgpack(self, func=None):
        gc.disable()
        for path in self.path.glob('*.msgpack'):
            with path.open('rb') as file:
                unpacker = msgpack.Unpacker(file)
                for record in unpacker:
                    if func is None:
                        yield record
                    else:
                        yield func(record)

    def optimise_json(self):
        for path in self.path.iterdir():
            print(path)
            with gzip.open(str(path), 'rt') as infile:
                with open(str(path) + '.msgpack', 'wb') as outfile:
                    for line in infile:
                        record = json.loads(line)
                        outfile.write(msgpack.packb(record))

    def count(self):
        iterator = self.iterate_msgpack(func=lambda event: event[b'type'])
        counter = Counter(iterator)
        return counter


def count():
    db = Database()
    events = Events()

    print(db.count())
    print(events.count())

    db.close()


if __name__ == '__main__':
    count()
