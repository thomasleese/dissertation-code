import sys

from apis import GitHub, Geography, Genderize
from database import User, session


def users(args):
    github = GitHub()

    users = github.get_all_users(int(args[0]))
    for github_user in users:
        github_user = github.get(github_user['url']).json()

        database_user = User()
        for field in ['id', 'login', 'avatar_url', 'gravatar_id', 'name',
                      'company', 'blog', 'location', 'email', 'hireable',
                      'bio']:
            setattr(database_user, field, github_user[field].strip())

        database_user = session.merge(database_user)
        session.add(database_user)
        session.commit()


def user_locations(args):
    geography = Geography()

    for user in session.query(User):
        if user.location is not None and user.location_country is None:
            try:
                country_code, country_name = \
                    geography.get_country(user.location)
            except ValueError:
                continue

            print('>', user.login, user.location, '->', country_code,
                  '({})'.format(country_name))
            user.location_country = country_code
            session.commit()


def user_genders(args):
    genderize = Genderize()

    for user in session.query(User):
        if user.name is not None and user.gender is None:
            try:
                gender, probability = genderize.guess(user.name.split()[0])
            except ValueError:
                continue

            print('>', user.login, user.name, '->', gender,
                  '({}%)'.format(probability * 100))
            user.gender = gender
            session.commit()


if __name__ == '__main__':
    name = sys.argv[1]
    args = sys.argv[2:]
    locals()[name](args)
