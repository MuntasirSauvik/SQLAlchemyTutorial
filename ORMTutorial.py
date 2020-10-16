import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String

# Version Check
print("Version Check")
print("sqlalchemy verion:"+ sqlalchemy.__version__  )

# Connecting , also import statement at line 2 needed
engine = create_engine('sqlite:///:memory:', echo=True)

# Declare a Mapping
Base = declarative_base()

class User(Base):
    __tablename__ = 'users'

    id = Column(Integer, primary_key=True)
    name = Column(String)
    fullname = Column(String)
    nickname = Column(String)

    def __repr__(self):
        return "<User(name='%s', fullname='%s', nickname='%s')>" % (self.name, self.fullname, self.nickname)

# Create a Schema
print("\n Create a Schema")
print(User.__table__)
Base.metadata.create_all(engine)

# Create an Instance of the Mapped Class
print("\nCreate an Instance of the Mapped Class")
ed_user = User(name='ed', fullname='Ed Jones', nickname='edsnickname')
print("ed_user.name:",ed_user.name)
print("ed_user.nickname:",ed_user.nickname)
print("ed_user.id:", str(ed_user.id))