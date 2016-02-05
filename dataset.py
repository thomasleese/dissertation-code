import gc
import gzip
import json
import msgpack
from pathlib import Path


DATASET_PATH = '../data'


"""def events(func=None):
    gc.disable()
    root_path = Path(DATASET_PATH)
    for path in root_path.iterdir():
        print(path)
        with gzip.open(str(path), 'rt') as file:
            for line in file:
                record = json.loads(line)
                if func is None:
                    yield record
                else:
                    yield func(record)"""


def events(func=None):
    gc.disable()
    root_path = Path(DATASET_PATH)
    for path in root_path.glob('*.msgpack'):
        with path.open('rb') as file:
            unpacker = msgpack.Unpacker(file)
            for record in unpacker:
                #print(record)
                if func is None:
                    yield record
                else:
                    yield func(record)


def optimise_json():
    root_path = Path(DATASET_PATH)
    for path in root_path.iterdir():
        print(path)
        with gzip.open(str(path), 'rt') as infile:
            with open(str(path) + '.msgpack', 'wb') as outfile:
                for line in infile:
                    record = json.loads(line)
                    outfile.write(msgpack.packb(record))


if __name__ == '__main__':
    #optimise_json()

    from collections import Counter

    counter = Counter(events(func=lambda event: event[b'type']))
    print(counter)
