from difflib import SequenceMatcher
import sys

import iso3166
from joblib import Memory
from matplotlib import cm
import matplotlib.pyplot as plt
import numpy as np
from PIL import Image

from dataset import Database, Events

memory = Memory('cache/analyse', verbose=0)


MATCHING_COMPANIES = {
    '.PROMO Inc': ['.PROMO Inc.'],
    'Abloom OG': ['abloom'],
    'Adobe': ['Adobe Systems', 'Adobe Systems Inc'],
    'Amazon': ['Amazon Web Services', 'Amazon.com'],
    'Apple': ['Apple Inc.', 'Apple, Inc.'],
    'Automattic': ['Automattic, Inc.'],
    'Baremetrics, Inc.': ['Baremetrics, Inc'],
    'Baobab Health': ['Baobab Health Trust'],
    'Basho Technologies': ['Basho Technologies, Inc.'],
    'Bekk Consulting': ['Bekk Consulting AS'],
    'Bloomberg L.P.': ['Bloomberg LP'],
    'Carbon Five': ['CarbonFive'],
    'Canonical': ['Canonical Ltd'],
    'Cisco': ['Cisco Systems', 'Cisco Systems, Inc.'],
    'Cloud Foundry': ['Pivotal / Cloud Foundry'],
    'Cookpad': ['Cookpad Inc', 'Cookpad, Inc', 'COOKPAD Inc.'],
    'Cognitect': ['Cognitect, Inc', 'Cognitect, Inc.'],
    'Color Technology, Inc.': ['Color Technology Inc'],
    'Custom Ink': ['CustomInk', 'CustomInk.com'],
    'Digital Ocean': ['DigitalOcean Inc.', 'DigitalOcean'],
    'Division By Zero': ['Division by Zero, LLC'],
    'Doximity': ['Doximity.com'],
    'Engine Yard': ['Engine Yard, Inc.', 'EngineYard'],
    'Expedia': ['Expedia.com'],
    'forward.co.uk': ['www.forward.co.uk'],
    'Freelance': ['Freelancer', 'Myself', 'My self', 'Independent',
                  'Self Employed', 'self-employed', 'HOME', 'none',
                  'n/a', 'NA', 'Self', 'Me', 'Consultant', '(Independent)',
                  'Independant'],
    'GitHub': ['GitHub, Inc.', 'GitHub Inc.'],
    'Go Free Range': ['Go Free Range Ltd'],
    'Google': ['Google Inc', 'Google Inc.', 'Google, Inc.'],
    'Heroku': ['Heroku, Inc.'],
    'Hewlett-Packard': ['Hewlett Packard'],
    'iCoreTech Inc.': ['iCoreTech, Inc.'],
    'innoQ': ['innoQ Deutschland GmbH'],
    'Insignia': ['(in)signia'],
    'Living Social': ['LivingSocial'],
    'MathWorks': ['The MathWorks'],
    'Mozilla': ['Mozilla Corporation'],
    'Netflix': ['Netflix, CA', 'Netflix DVD'],
    'Nitrous': ['Nitrous.IO'],
    'Planet Argon': ['Planet Argon, LLC'],
    'Red Hat': ['Red Hat, Inc.'],
    'Scribd': ['Scribd.'],
    'SoundCloud': ['SoundCloud Ltd.'],
    'Skroutz S.A.': ['Skroutz S.A'],
    'Spotify': ['Spotify AB'],
    'Square': ['Square, Inc.'],
    'Square Mill Labs': ['SquareMill Labs'],
    'Swiftype': ['Swiftype.com'],
    'Technology Astronauts': ['Technology Astronauts GmbH'],
    'The New York Times': ['The New York Times / Graphics'],
    'thoughtbot': ['thoughtbot, inc.'],
    'ThoughtWorks': ['ThoughtWorks Inc.', 'ThoughtWorks, Inc.'],
    'Twitter': ['Twitter, Inc.'],
    'Upworthy': ['Upworthy.com'],
    'UserVoice': ['User Voice'],
    'uSwitch': ['uSwitch.com'],
    'Vox Media': ['Vox Media, Inc'],
    'Yahoo!': ['Yahoo'],
    'Zendesk': ['Zendesk.com'],
    'Zetetic LLC': ['Zetetic, LLC'],
}


def companies():
    database = Database()
    data = database.get_company_distribution()

    def find_similarities():
        names = list(data.keys())
        for i, a in enumerate(names):
            for b in names[i + 1:]:
                ratio = SequenceMatcher(None, a, b).ratio()
                if ratio >= 0.75:
                    print("'{}'".format(a), "'{}'".format(b), ratio)

    # sort matching companies
    for a, bs in MATCHING_COMPANIES.items():
        for b in bs:
            data[a] += data[b]
            del data[b]

    # find_similarities()

    # draw graph
    for name, count in list(data.items()):
        if count <= 3:
            del data[name]
        # print(name, count)

    names = list(data.keys())
    counts = list(data.values())
    positions = np.arange(len(names))

    plt.figure(figsize=(70, 12), dpi=120)
    plt.bar(positions, counts)
    plt.xlim([0, len(names)])
    plt.xticks(positions + 0.4, names, rotation='vertical')
    plt.subplots_adjust(bottom=0.34, left=0.02, right=0.98)
    plt.savefig('results/companies.png')


def countries():
    database = Database()
    data = database.get_country_distribution()

    for code in list(data.keys()):
        try:
            country = iso3166.countries.get(code)
        except KeyError:
            continue
        else:
            data[country.apolitical_name] = data[code]
            del data[code]

    names = list(data.keys())
    counts = list(data.values())
    positions = np.arange(len(names))

    plt.figure(figsize=(30, 12), dpi=120)
    plt.bar(positions, counts)
    plt.xlim([0, len(names)])
    plt.xticks(positions + 0.4, names, rotation='vertical')
    plt.subplots_adjust(bottom=0.2, left=0.02, right=0.98)
    plt.savefig('results/countries.png')


def genders():
    database = Database()
    data = database.get_gender_distribution()

    mappings = [
        ('Male', 'M'),
        ('Female', 'F'),
        ('Unknown', '?')
    ]

    for mapping in mappings:
        data[mapping[0]] = data[mapping[1]]
        del data[mapping[1]]

    names = list(data.keys())
    counts = list(data.values())
    positions = np.arange(len(names))

    plt.figure(figsize=(5, 20), dpi=120)
    plt.bar(positions, counts)
    plt.xlim([0, len(names)])
    plt.xticks(positions + 0.4, names, rotation='vertical')
    plt.subplots_adjust(bottom=0.1, left=0.1, right=0.9)
    plt.savefig('results/genders.png')


def world_map():
    width = 3422
    height = 1731

    image = Image.open('resources/world-map.png')
    point = Image.open('resources/point.png')

    database = Database()
    for lat, lon in database.get_location_points():
        lat = float(lat)
        lon = float(lon)

        x = int((lon + 180) * (width / 360))
        y = height - int((lat + 90) * (height / 180))

        image.paste(point, (x - 8, y - 8, x + 8, y + 8), point)

    image.save('results/world_map.png')


def growth():
    events = Events()

    """
    p1 = plt.bar(ind, menMeans, width, color='r', yerr=menStd)
p2 = plt.bar(ind, womenMeans, width, color='y',
             bottom=menMeans, yerr=womenStd)

plt.ylabel('Scores')
plt.title('Scores by group and gender')
plt.xticks(ind + width/2., ('G1', 'G2', 'G3', 'G4', 'G5'))
plt.yticks(np.arange(0, 81, 10))
plt.legend((p1[0], p2[0]), ('Men', 'Women'))

plt.show()
"""

    n = 12 * 6

    types = list(sorted(events.types))

    data = {s: [] for s in types}

    for year in range(2011, 2017):
        for month in range(1, 13):
            print(year, month)
            counts = events.count_types(year, month)
            print(counts)
            for key in data.keys():
                data[key].append(counts[key])

    ind = np.arange(n)

    plt.figure(figsize=(30, 15), dpi=120)

    legend = {}
    for i, (key, value) in enumerate(data.items()):
        legend[key] = plt.bar(ind, value, 0.5, color=cm.jet(i / len(data)))

    plt.legend(legend.values(), legend.keys())

    plt.xlim([0, len(data)])
    plt.savefig('results/growth.png')


if __name__ == '__main__':
    for name in sys.argv[1:]:
        print('Analysing:', name)
        locals()[name]()
