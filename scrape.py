import sys

from database import User, session
import github


users = github.get_all_users(int(sys.argv[1]))
for github_user in users:
    github_user = github.get(github_user['url']).json()

    database_user = User()
    for field in ['id', 'login', 'avatar_url', 'gravatar_id', 'name',
                  'company', 'blog', 'location', 'email', 'hireable', 'bio']:
        setattr(database_user, field, github_user[field])

    database_user = session.merge(database_user)
    session.add(database_user)
    session.commit()
