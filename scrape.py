import sys

from sqlalchemy import desc, or_

from apis import GitHub, Geography, Genderize
from database import User, session


def users():
    github = GitHub()

    last_user_id = session.query(User.id).order_by(desc(User.id)).limit(1) \
        .first()[0]

    users = github.get_all_users(last_user_id)
    for github_user in users:
        github_user = github.get(github_user['url']).json()

        database_user = User()

        database_user.id = github_user['id']
        database_user.hireable = github_user['hireable']

        for field in ['login', 'avatar_url', 'gravatar_id', 'name', 'company',
                      'blog', 'location', 'email', 'bio']:
            if github_user[field] is None:
                setattr(database_user, field, None)
            else:
                setattr(database_user, field, github_user[field].strip())

        database_user = session.merge(database_user)
        session.add(database_user)
        session.commit()


def user_followings():
    github = GitHub()

    """for user in session.query(User):
        for other_user in github.get_following_users(user.login):
            print(other_user)"""


def user_locations():
    geography = Geography()

    query = session.query(User).filter(User.location != None) \
        .filter(or_(User.location_country == None,
                    User.location_latitude == None,
                    User.location_longitude == None))

    for user in query:
        location = geography.geocode(user.location)
        if location is None:
            continue

        try:
            country_code, country_name = geography.get_country(location)
        except ValueError:
            continue

        print('>', '#{}'.format(user.id), user.login, user.location, '->',
              country_code, '({})'.format(country_name))

        user.location_country = country_code
        user.location_latitude = location.latitude
        user.location_longitude = location.longitude

        session.commit()


def user_genders():
    genderize = Genderize()

    query = session.query(User).filter(User.name != None) \
        .filter(User.gender == None)

    for user in query:
        try:
            gender, probability = genderize.guess(user.name.split()[0])
        except ValueError:
            continue

        print('>', '#{}'.format(user.id), user.login, user.name, '->',
              gender, '({}%)'.format(probability * 100))
        user.gender = gender
        session.commit()


if __name__ == '__main__':
    for name in sys.argv[1:]:
        locals()[name]()
