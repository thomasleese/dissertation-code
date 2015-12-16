from collections import OrderedDict
from difflib import SequenceMatcher
import math
import sys

import iso3166
import matplotlib.pyplot as plt
import numpy as np
from PIL import Image
from sqlalchemy import func

from database import User, session


MATCHING_COMPANIES = {
    None: ['private'],
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
                  'n/a', 'NA', 'Self', 'Me', 'Consultant'],
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
    data = OrderedDict(session.query(User.company, func.count(User.id))
                       .group_by(User.company))

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

    del data[None]

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
    plt.savefig('companies.png')


def countries():
    data = OrderedDict(session.query(User.location_country,
                                     func.count(User.id))
                       .group_by(User.location_country))
    del data[None]

    for code in list(data.keys()):
        country = iso3166.countries.get(code)
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
    plt.savefig('countries.png')


def genders():
    data = OrderedDict(session.query(User.gender, func.count(User.id))
                       .group_by(User.gender))
    del data[None]

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
    plt.savefig('genders.png')


if __name__ == '__main__':
    for name in sys.argv[1:]:
        locals()[name]()
