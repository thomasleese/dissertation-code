import gzip
import json
from pathlib import Path


DATASET_PATH = '../data'


def events(func=None):
    root_path = Path(DATASET_PATH)
    for path in root_path.iterdir():
        with gzip.open(str(path), 'rt') as file:
            for line in file:
                record = json.loads(line)
                if func is None:
                    yield record
                else:
                    yield func(record)


if __name__ == '__main__':
    from collections import Counter

    counter = Counter(events(func=lambda event: event['type']))
    print(counter)
