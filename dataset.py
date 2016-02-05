from collections import Counter
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
