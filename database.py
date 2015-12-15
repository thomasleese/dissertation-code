import os

from sqlalchemy import create_engine, Boolean, Column, Integer, String, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker


DATABASE_URI = os.environ['DATABASE_URI']

Base = declarative_base()


class User(Base):
    __tablename__ = 'users'

    id = Column(Integer, primary_key=True)
    login = Column(String(100))
    avatar_url = Column(String(200))
    gravatar_id = Column(String(100))
    name = Column(String(100))
    company = Column(String(100))
    blog = Column(String(200))
    location = Column(String(100))
    email = Column(String(200))
    hireable = Column(Boolean)
    bio = Column(Text)


engine = create_engine(DATABASE_URI)
Session = sessionmaker(bind=engine)

Base.metadata.create_all(engine)

session = Session()
