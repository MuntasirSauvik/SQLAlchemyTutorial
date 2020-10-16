import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import sessionmaker

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

# Creating a Session
print("\nCreating a Session")
Session = sessionmaker(bind=engine)
session = Session()
print(session)

# Adding and Updating Objects
print("\nAdding and Updating Objects")
# ed_user = User(name='ed', fullname='Ed Jones', nickname='edsnickname')
session.add(ed_user)
our_user = session.query(User).filter_by(name='ed').first()
print(our_user)
print("ed_user is our_user:", ed_user is our_user)

# We can add more User objects at once using add_all():
print("\nWe can add more User objects at once using add_all():")
session.add_all([
    User(name='wendy', fullname='Wendy Williams', nickname='windy'),
    User(name='mary', fullname='Mary Contrary', nickname='mary'),
    User(name='fred', fullname='Fred Flintstone', nickname='freddy')])

# Also, we’ve decided Ed’s nickname isn’t that great, so lets change it:
print("\nAlso, we’ve decided Ed’s nickname isn’t that great, so lets change it:")
print("ed_user.nickname before: ", ed_user.nickname)
ed_user.nickname = 'eddie'
print("ed_user.nickname After change: ", ed_user.nickname)

# The Session is paying attention. It knows, for example, that Ed Jones has been modified:
print("\nThe Session is paying attention. It knows, for example, that Ed Jones has been modified:")
print(session.dirty)

# and that three new User objects are pending:
print("\nand that three new User objects are pending:")
print(session.new)

# We tell the Session that we’d like to issue all remaining changes to the database and commit the transaction, which
# has been in progress throughout.
print("\nWe tell the Session that we’d like to issue all remaining changes to the database and commit the transaction, "
      "which has been in progress throughout. ")
session.commit()

# If we look at Ed’s id attribute, which earlier was None, it now has a value:
print("\nIf we look at Ed’s id attribute, which earlier was None, it now has a value:")
print("ed_user.id: ", ed_user.id)
