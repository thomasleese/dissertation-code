from collections import OrderedDict
from difflib import SequenceMatcher

import matplotlib.pyplot as plt
import numpy as np
from sqlalchemy import func

from database import User, session


MATCHING_COMPANIES = {
    'GitHub': ['GitHub, Inc.'],
    'Apple': ['Apple Inc.', 'Apple, Inc.'],
    'Yahoo!': ['Yahoo'],
    'Twitter': ['Twitter, Inc.'],
    'Freelance': ['Freelancer', 'Myself', 'My self', 'Independent',
                  'Self Employed', 'self-employed', 'HOME', 'none',
                  'n/a', 'NA', 'Self', 'Me'],
    'thoughtbot': ['thoughtbot, inc.'],
    'Abloom OG': ['abloom'],
    'Adobe Systems': ['Adobe Systems Inc'],
    'Google': ['Google Inc', 'Google Inc.', 'Google, Inc.'],
    'Basho Technologies': ['Basho Technologies, Inc.'],
    'Canonical': ['Canonical Ltd'],
    'Cookpad Inc': ['Cookpad, Inc'],
    'Custom Ink': ['CustomInk', 'CustomInk.com'],
    'Digital Ocean': ['DigitalOcean Inc.', 'DigitalOcean'],
    'Division By Zero': ['Division by Zero, LLC'],
    'Doximity': ['Doximity.com'],
    'forward.co.uk': ['www.forward.co.uk'],
    'Go Free Range': ['Go Free Range Ltd'],
    'Living Social': ['LivingSocial'],
    'Cisco': ['Cisco Systems', 'Cisco Systems, Inc.'],
    'Planet Argon': ['Planet Argon, LLC'],
    'Scribd': ['Scribd.'],
    'Skroutz S.A.': ['Skroutz S.A'],
    'Square Mill Labs': ['SquareMill Labs'],
    'ThoughtWorks': ['ThoughtWorks Inc.', 'ThoughtWorks, Inc.'],
    'UserVoice': ['User Voice'],
    'Upworthy': ['Upworthy.com'],
    'Automattic': ['Automattic, Inc.'],
    'Baremetrics, Inc.': ['Baremetrics, Inc'],
    'Netflix': ['Netflix, CA'],
    'Netflix': ['Netflix DVD'],
    'SoundCloud': ['SoundCloud Ltd.'],
    'Spotify': ['Spotify AB'],
    'Vox Media': ['Vox Media, Inc'],
    'uSwitch': ['uSwitch.com'],
    'The New York Times': ['The New York Times / Graphics'],
    'Technology Astronauts': ['Technology Astronauts GmbH'],
    'Square': ['Square, Inc.'],
    'Red Hat': ['Red Hat, Inc.'],
    'innoQ': ['innoQ Deutschland GmbH'],
    'Heroku': ['Heroku, Inc.']
}

companies = OrderedDict(session.query(User.company, func.count(User.id))
                        .group_by(User.company)
                        .all())


def find_similarities():
    names = list(companies.keys())
    for i, a in enumerate(names):
        for b in names[i + 1:]:
            ratio = SequenceMatcher(None, a, b).ratio()
            if ratio >= 0.75:
                print("'{}'".format(a), "'{}'".format(b), ratio)


# sort matching companies
for a, bs in MATCHING_COMPANIES.items():
    for b in bs:
        try:
            companies[a] += companies[b]
        except KeyError:
            companies[a] = companies[b]

        del companies[b]

print(companies[None])
del companies[None]

# find_similarities()

# draw graph
for name, count in list(companies.items()):
    if count <= 1:
        del companies[name]
    # print(name, count)


names = list(companies.keys())
counts = list(companies.values())
positions = np.arange(len(names))

plt.figure(figsize=(70, 12), dpi=120)
plt.bar(positions, counts)
plt.xlim([0, len(names)])
plt.xticks(positions + 0.4, names, rotation='vertical')
plt.subplots_adjust(bottom=0.3, left=0.02, right=0.98)
plt.savefig('companies.png')
