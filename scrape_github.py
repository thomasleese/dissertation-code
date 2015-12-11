import os

import sqlalchemy


database_engine = sqlalchemy.create_engine(os.environ['DATABASE_URI'])
