import sys

from apis import GitHub, Geography, Genderize
from dataset import Database


def print_status(user_id, *args):
    print('>', '#{}'.format(user_id), *args)
    sys.stdout.flush()


def users():
    github = GitHub()
    database = Database()

    with database.cursor() as cursor:
        cursor.execute('SELECT id FROM users ORDER BY id DESC LIMIT 1')
        last_user_id = cursor.fetchone()[0]

    users = github.get_all_users(last_user_id)
    for github_user in users:
        github_user = github.get(github_user['url']).json()
        if 'id' not in github_user:
            continue  # not a user

        fields = {
            'id': github_user['id'],
            'hireable': github_user['hireable']
        }

        for field in ['login', 'avatar_url', 'gravatar_id', 'name', 'company',
                      'blog', 'location', 'email', 'bio']:
            if github_user[field] is None:
                fields[field] = None
            else:
                fields[field] = github_user[field].strip()

        with database.cursor() as cursor:
            fields_str = ', '.join(fields.keys())
            values_str = ', '.join(['%s'] * len(fields))
            sql = 'INSERT INTO users ({}) VALUES ({})' \
                .format(fields_str, values_str)
            cursor.execute(sql, tuple(fields.values()))

        database.commit()

        print_status(fields['id'], fields['login'], '✓')

    database.close()


def user_followings():
    github = GitHub()

    """for user in session.query(User):
        for other_user in github.get_following_users(user.login):
            print(other_user)"""


def user_locations():
    geography = Geography()
    database = Database()

    with database.cursor() as cursor:
        cursor.execute("""
            SELECT id, location
            FROM users
            WHERE location IS NOT NULL
                AND (location_country IS NULL
                    OR location_latitude IS NULL
                    OR location_longitude IS NULL)
        """)

        for user_id, location_str in cursor:
            location = geography.geocode(location_str)
            if location is None:
                continue

            try:
                country_code, country_name = geography.get_country(location)
            except ValueError:
                continue

            print_status(user_id, location_str, '->', country_code,
                         '({})'.format(country_name))

            with database.cursor() as cursor2:
                args = (country_code, location.latitude, location.longitude,
                        user_id)
                cursor2.execute("""
                    UPDATE users
                    SET location_country = %s,
                        location_latitude = %s,
                        location_longitude = %s
                    WHERE id = %s
                """, args)

            database.commit()

    database.close()


def user_genders():
    genderize = Genderize()
    database = Database()

    with database.cursor() as cursor:
        cursor.execute("""
            SELECT id, name
            FROM users
            WHERE name IS NOT NULL
                AND (gender IS NULL OR gender != '?')
                AND gender_probability IS NULL
        """)

        for user_id, name in cursor:
            gender, probability = genderize.guess(name.split()[0])

            try:
                probability_str = '({}%)'.format(probability * 100)
            except TypeError:
                probability_str = '(N/A)'

            print_status(user_id, name, '->', gender, probability_str)

            with database.cursor() as cursor2:
                args = (gender, probability, user_id)
                cursor2.execute("""
                    UPDATE users
                    SET gender = %s, gender_probability = %s
                    WHERE id = %s
                """, args)

            database.commit()

    database.close()


if __name__ == '__main__':
    for name in sys.argv[1:]:
        locals()[name]()
