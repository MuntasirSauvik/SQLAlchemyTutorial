import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import aliased
from sqlalchemy import text
from sqlalchemy import func
from sqlalchemy import ForeignKey
from sqlalchemy.orm import relationship

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

# Returning Lists and Scalars
print("\nReturning Lists and Scalars:")
print("A number of methods on Query immediately issue SQL and return a value containing loaded database results.")
print("Query.all() returns a list:")
query = session.query(User).filter(User.name.like('%ed')).order_by(User.id)
print(query.all())
print("Query.first() applies a limit of one and returns the first result as a scalar:")
print(query.first())
print("Query.one() fully fetches all rows, and if not exactly one object identity or composite row is present in the "
      "result, raises an error. With multiple rows found:")
# user = query.one()
# print(user)
print("\nQuery.scalar() invokes the Query.one() method, and upon success returns the first column of the row:")
query = session.query(User.id).filter(User.name == 'ed').order_by(User.id)
print(query.scalar())

# Using Textual SQL

print("Literal strings can be used flexibly with Query, by specifying their use with the text() construct, which is "
      "accepted by most applicable methods. For example, Query.filter() and Query.order_by():")
for user in session.query(User).filter(text("id<224")).order_by(text("id")).all():
    print(user.name)

print("\nBind parameters can be specified with string-based SQL, using a colon. To specify the values, use the "
      "Query.params() method:")
print(session.query(User).filter(text("id<:value and name=:name")).params(value=224, name='fred').order_by(User.id).one())

print("To use an entirely string-based statement, a text() construct representing a complete statement can be passed to"
      "Query.from_statement(). ")
print(session.query(User).from_statement(
    text("SELECT * FROM users where name=:name")).params(name='ed').all())
print("\nthe text() construct allows us to link its textual SQL to Core or ORM-mapped column expressions positionally; "
      "we can achieve this by passing column expressions as positional arguments to the TextClause.columns() method:")
stmt = text("SELECT name, id, fullname, nickname FROM users where name=:name")
stmt = stmt.columns(User.name, User.id, User.fullname, User.nickname)
print(session.query(User).from_statement(stmt).params(name='ed').all())

print("\nWhen selecting from a text() construct, the Query may still specify what columns and entities are to be "
      "returned; instead of query(User) we can also ask for the columns individually, as in any other case:")
stmt = text("SELECT name, id FROM users where name=:name")
stmt = stmt.columns(User.name, User.id)
print(session.query(User.id, User.name).from_statement(stmt).params(name='ed').all())

# Counting
print("\nCounting")
print("Query includes a convenience method for counting called Query.count():")
print("session.query(User).filter(User.name.like('%ed')).count():",session.query(User).filter(User.name.like('%ed')).count())

print("\nsession.query(func.count(User.name), User.name).group_by(User.name).all():",session.query(func.count(User.name), User.name).group_by(User.name).all())
print("\nsession.query(func.count('*')).select_from(User).scalar():",session.query(func.count('*')).select_from(User).scalar())
print("\nThe usage of Query.select_from() can be removed if we express the count in terms of the User primary key directly:")
print("session.query(func.count(User.id)).scalar():",session.query(func.count(User.id)).scalar())


# Building a Relationship
print("\nBuilding a Relationship")
class Address(Base):
    __tablename__ = 'addresses'
    id = Column(Integer, primary_key=True)
    email_address = Column(String, nullable=False)
    user_id = Column(Integer, ForeignKey('users.id'))

    user = relationship("User", back_populates="addresses")

    def __repr__(self):
        return "<Address(email_address='%s')>" % self.email_address

User.addresses = relationship("Address", order_by=Address.id, back_populates="user")

print("\nWe’ll need to create the addresses table in the database, so we will issue another CREATE from our metadata, "
      "which will skip over tables which have already been created:")
Base.metadata.create_all(engine)

# Working with related Objects
print("\nWorking with related Objects:")
print("\nNow when we create a User, a blank addresses collection will be present. Various collection types, such as "
      "sets and dictionaries, are possible here ")
jack = User(name='jack', fullname='Jack Bean', nickname='gjffdd')
print("jack:",jack)
print("jack.addresses:",jack.addresses)

print("\nWe are free to add Address objects on our User object. In this case we just assign a full list directly:")
print("jack.addresses before assignment:",jack.addresses)
jack.addresses =[
                    Address(email_address='jack@google.com'),
                    Address(email_address='j25@yahoo.com')]
print("jack.addresses after assignment:",jack.addresses)

print("\nDemonstrating bidirectional relationship:")
print("jack.addresses[1]:",jack.addresses[1])
print("jack.addresses[1].user:",jack.addresses[1].user)

print("\nLet’s add and commit Jack Bean to the database. jack as well as the two Address members in the corresponding "
      "addresses collection are both added to the session at once, using a process known as cascading:")
session.add(jack)
session.commit()

print("\nQuerying for Jack, we get just Jack back. No SQL is yet issued for Jack’s addresses:")
jack = session.query(User).filter_by(name='jack').one()
print("Querying before lazy loading:\njack:",jack)
print("Querying for Jack, we get just Jack back. No SQL is yet issued for Jack’s addresses:")
print("\nLet’s look at the addresses collection. Watch the SQL:")
print("jack.addresses:",jack.addresses)

# Querying with Joins
print("\nQuerying with Joins:")
print("To construct a simple implicit join between User and Address, we can use Query.filter() to equate their related "
      "columns together. Below we load the User and Address entities at once using this method:")

for u, a in session.query(User, Address).filter(
                                        User.id==Address.user_id).filter(
                                        Address.email_address=='jack@google.com').all():
    print("u:",u)
    print("a:",a)

print("\nThe actual SQL JOIN syntax, on the other hand, is most easily achieved using the Query.join() method:")
print(session.query(User, Address).join(Address).filter(
                                        Address.email_address=='jack@google.com').all())
