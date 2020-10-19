import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import aliased

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

# Rolling Back
print("\nRolling back")
# Let’s make two changes that we’ll revert; ed_user’s user name gets set to Edwardo:
print("\nLet’s make two changes that we’ll revert; ed_user’s user name gets set to Edwardo:")
print("ed_user.name before change: ",ed_user.name)
ed_user.name = 'Edwardo'
print("ed_user.name after change: ",ed_user.name)

# we’ll add another erroneous user, fake_user:
print("\nwe’ll add another erroneous user, fake_user:")
fake_user = User(name='fakeuser', fullname='Invalid', nickname='12345')
session.add(fake_user)

# Querying the session, we can see that they’re flushed into the current transaction:
print("\nQuerying the session, we can see that they’re flushed into the current transaction:")
print(session.query(User).filter(User.name.in_(['Edwardo', 'fakeuser'])).all())

# Rolling back, we can see that ed_user’s name is back to ed, and fake_user has been kicked out of the session:
print("\nRolling back, we can see that ed_user’s name is back to ed, and fake_user has been kicked out of the session:")
session.rollback()
print("ed_user.name: ", ed_user.name)
print("fake_user in session: ", fake_user in session)

# issuing a SELECT illustrates the changes made to the database:
print("\nissuing a SELECT illustrates the changes made to the database:")
print(session.query(User).filter(User.name.in_(['ed', 'fakeuser'])).all())

# Querying
print("\nQuerying")
for instance in session.query(User).order_by(User.id):
    print(instance.name, instance.fullname)

# The Query also accepts ORM-instrumented descriptors as arguments. Any time multiple class entities or column-based
# entities are expressed as arguments to the query() function, the return result is expressed as tuples:
print("\nThe Query also accepts ORM-instrumented descriptors as arguments. Any time multiple class entities or column-"
      "based entities are expressed as arguments to the query() function, the return result is expressed as tuples:")
for name, fullname in session.query(User.name, User.fullname):
    print(name, fullname)
# The tuples returned by Query are named tuples, supplied by the KeyedTuple class, and can be treated much like an
# ordinary Python object. The names are the same as the attribute’s name for an attribute, and the class name for a
# class:
print("\nThe tuples returned by Query are named tuples, supplied by the KeyedTuple class, and can be treated much like "
      "an ordinary Python object. The names are the same as the attribute’s name for an attribute, and the class name "
      "for a class:")
for row in session.query(User, User.name).all():
    print(row.User, row.name)

# You can control the names of individual column expressions using the ColumnElement.label() construct, which is
# available from any ColumnElement-derived object, as well as any class attribute which is mapped to one (such as
# User.name):
print("\nYou can control the names of individual column expressions using the ColumnElement.label() construct, which is"
      "available from any ColumnElement-derived object, as well as any class attribute which is mapped to one (such as "
      "User.name):")
for row in session.query(User.name.label('name_label')).all():
    print(row.name_label)

# Using alias
print("\nUsing Alias:")
user_alias = aliased(User, name='user_alias')
for row in session.query(user_alias, user_alias.name).all():
    print(row.user_alias)

# Basic operations with Query include issuing LIMIT and OFFSET, most conveniently using Python array slices and
# typically in conjunction with ORDER BY:
print("\nBasic operations with Query include issuing LIMIT and OFFSET, most conveniently using Python array slices and "
      "typically in conjunction with ORDER BY:")
print("for u in session.query(User).order_by(User.id)[1:3]:"
      "\n     print(u)")
for u in session.query(User).order_by(User.id)[1:3]:
    print(u)

#  filtering results, which is accomplished either with filter_by(), which uses keyword arguments:
print("\nfiltering results, which is accomplished either with filter_by(), which uses keyword arguments:")
print("for name, in session.query(User.name).filter_by(fullname='Ed Jones'):"
        "\n   print(name)")
for name, in session.query(User.name).filter_by(fullname='Ed Jones'):
    print(name)

# filter(), which uses more flexible SQL expression language constructs. These allow you to use regular Python operators
# with the class-level attributes on your mapped class:
print("\n# filter(), which uses more flexible SQL expression language constructs. These allow you to use regular Python"
      "operators with the class-level attributes on your mapped class:")
print("for name, in session.query(User.name).filter(User.fullname=='Ed Jones'):"
        "\n    print(name)")
for name, in session.query(User.name).filter(User.fullname=='Ed Jones'):
    print(name)

# to query for users named “ed” with a full name of “Ed Jones”, you can call filter() twice, which joins criteria using
# AND:
print("\nto query for users named “ed” with a full name of “Ed Jones”, you can call filter() twice, which joins "
      "criteria using AND:")
print("for user in session.query(User).filter(User.name=='ed').filter(User.fullname=='Ed Jones'):"
        "\n    print(user)")
for user in session.query(User).filter(User.name=='ed').filter(User.fullname=='Ed Jones'):
    print(user)

# Common Filter Operators
print("\nCommon Filter Operators:")
print("\nHere’s a rundown of some of the most common operators used in filter():")
print("ColumnOperators.__eq__():\nquery.filter(User.name == 'ed'):")
print("ColumnOperators.__ne__():\nquery.filter(User.name != 'ed')")
print("ColumnOperators.like():\nquery.filter(User.name.like('%ed%'))")
print("ColumnOperators.ilike() (case-insensitive LIKE):\nquery.filter(User.name.queryilike('%ed%'))")
print("ColumnOperators.notin_():\nquery.filter(~User.name.in_(['ed', 'wendy', 'jack']))")
print("ColumnOperators.is_():\nquery.filter(User.name == None)")
print("alternatively, if pep8/linters are a concern:\nquery.filter(User.name.is_(None))")
print("More examples are on the tutorial page")