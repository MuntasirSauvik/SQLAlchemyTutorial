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
from sqlalchemy.orm import aliased
from sqlalchemy.sql import func
from sqlalchemy.sql import exists
from sqlalchemy.orm import selectinload
from sqlalchemy.orm import joinedload
from sqlalchemy.orm import contains_eager

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

# Using Aliases
print("\nUsing Aliases:")
adalias1 = aliased(Address)
adalias2 = aliased(Address)

for username, email1, email2 in \
    session.query(User.name, adalias1.email_address, adalias2.email_address).\
    join(User.addresses.of_type(adalias1)).\
    join(User.addresses.of_type(adalias2)).\
    filter(adalias1.email_address=='jack@google.com').\
    filter(adalias2.email_address=='j25@yahoo.com'):
    print(username, email1, email2)

# Using Subqueries
print("\nUsing Subqueries:")
stmt = session.query(Address.user_id, func.count('*').\
    label('address_count')).\
    group_by(Address.user_id).subquery()
print(stmt,"\n")
for u, count in session.query(User, stmt.c.address_count).\
    outerjoin(stmt, User.id==stmt.c.user_id).order_by(User.id):
    print(u, count)

# Selecting Entities from Subqueries
print("\nSeleceting Entities from Subqueries:")
stmt = session.query(Address).\
        filter(Address.email_address != 'j25@yahoo.com').\
        subquery()
adalias = aliased(Address, stmt)
for user, address in session.query(User, adalias).\
        join(adalias, User.addresses):
    print(user)
    print(address)

# Using EXISTS
print("\nUsing EXISTS:")
print("\nThere is an explicit EXISTS construct, which looks like this:")
print("stmt = exists().where(Address.user_id==User.id)")
stmt = exists().where(Address.user_id==User.id)
for name, in session.query(User.name).filter(stmt):
    print("name:",name)

print("\nThe Query features several operators which make usage of EXISTS automatically. Above, the statement can be "
      "expressed along the User.addresses relationship using Comparator.any():")
for name, in session.query(User.name).\
    filter(User.addresses.any()):
    print("name:",name)

print("\nComparator.any() takes criterion as well, to limit the rows matched:")
for name, in session.query(User.name).\
    filter(User.addresses.any(Address.email_address.like('%google%'))):
    print("name:",name)

print("\nComparator.has() is the same operator as Comparator.any() for many-to-one relationships (note the ~ operator "
      "here too, which means “NOT”):")
print("session.query(Address).filter(~Address.user.has(User.name=='jack')).all(): ",session.query(Address).\
                                                        filter(~Address.user.has(User.name=='jack')).all())

# Common Relationship Operators
print("\nCommon Relationship Operators:")

print("\nComparator.__eq__()(many - to - one “equals” comparison):")
print("query.filter(Address.user == someuser)")

print("\nComparator.__ne__()(many - to - one “not equals” comparison):")
print("query.filter(Address.user != someuser)")

print("\nIS NULL(many - to - one comparison, also uses Comparator.__eq__()):")
print("query.filter(Address.user == None)")

print("\nComparator.contains()(used for one - to - many collections):")
print("query.filter(User.addresses.contains(someaddress))")

print("\nComparator.any()(used for collections):")
print("query.filter(User.addresses.any(Address.email_address == 'bar'))")
print("also takes keyword arguments:")
print("query.filter(User.addresses.any(email_address='bar'))")

print("\nComparator.has()(used for scalar references):")
print("query.filter(Address.user.has(name='ed'))")

print("\nQuery.with_parent()(used for any relationship):")
print("session.query(Address).with_parent(someuser, 'addresses')")

# Eager Loading
print("\nEager Loading:")
print("Selectin Load:")
jack = session.query(User).\
        options(selectinload(User.addresses)).\
        filter_by(name='jack').one()
print("jack: ", jack)
print("jack.addresses: ",jack.addresses)

# Joined Load
print("\nJoined Load:")
jack = session.query(User). \
        options(joinedload(User.addresses)). \
        filter_by(name='jack').one()
print("jack: ", jack)
print("jack.addresses: ",jack.addresses)

# Explicit Join + Eagerload
print("\nExplicit Join + Eagerload:")
print("Below we illustrate loading an Address row as well as the related User object, filtering on the User named "
      "“jack” and using contains_eager() to apply the “user” columns to the Address.user attribute:")
jacks_addresses = session.query(Address).\
                                join(Address.user).\
                                filter(User.name=='jack').\
                                options(contains_eager(Address.user)).\
                                all()
print("jacks_addresses: ", jacks_addresses)
print("jacks_addresses[0].user: ", jacks_addresses[0].user)

# Deleting
print("\nDeleting:")
print("Let’s try to delete jack and see how that goes. We’ll mark the object as deleted in the session, then we’ll "
      "issue a count query to see that no rows remain::")
session.delete(jack)
print("session.query(User).filter_by(name='jack').count(): ", session.query(User).filter_by(name='jack').count())

print("\nSo far, so good. How about Jack’s Address objects ?:")
print("session.query(Address).filter(Address.email_address.in_(['jack@google.com', 'j25@yahoo.com'])).count():",
      session.query(Address).filter(Address.email_address.in_(['jack@google.com', 'j25@yahoo.com'])).count())

# Configuring delete/delete-orphan Cascade
print("\nConfiguring delete/delete-orphan Cascade:")
print("While SQLAlchemy allows you to add new attributes and relationships to mappings at any point in time, in this "
      "case the existing relationship needs to be removed, so we need to tear down the mappings completely and start "
      "again - we’ll close the Session:")
print("\nsession.close(): ")
session.close()

print("\nAnd use a new declarative_base():")
print("Base = declarative_base()")
Base = declarative_base()
print("\nNext we’ll declare the User class, adding in the addresses relationship including the cascade configuration "
      "(we’ll leave the constructor out too):")
class User(Base):
    __tablename__ = 'users'

    id = Column(Integer, primary_key=True)
    name = Column(String)
    fullname = Column(String)
    nickname = Column(String)

    addresses = relationship("Address", back_populates='user',
                        cascade="all, delete, delete-orphan")

    def __repr__(self):
        return "<User(name='%s', fullname='%s', nickname='%s')>" % (
                                self.name, self.fullname, self.nickname)

print("\nThen we recreate Address, noting that in this case we’ve created the Address.user relationship via the User "
      "class already:")
class Address(Base):
    __tablename__ = 'addresses'
    id = Column(Integer, primary_key=True)
    email_address = Column(String, nullable=False)
    user_id = Column(Integer, ForeignKey('users.id'))
    user = relationship("User", back_populates="addresses")

    def __repr__(self):
        return "<Address(email_address='%s')>" % self.email_address
print("\nNow when we load the user jack (below using Query.get(), which loads by primary key), removing an address "
      "from the corresponding addresses collection will result in that Address being deleted:")
print("load Jack by primary key")
print("jack = session.query(User).get(5)")
jack = session.query(User).get(5)
print("jack = ", jack)

print("\nremove one Address (lazy load fires off)")
print("del jack.addresses[1]")
del jack.addresses[1]

print("\nonly one address remains")
print("session.query(Address).filter(Address.email_address.in_(['jack@google.com', 'j25@yahoo.com'])).count(): ",
        session.query(Address).filter(Address.email_address.in_(['jack@google.com', 'j25@yahoo.com'])).count())

print("\nDeleting Jack will delete both Jack and the remaining Address associated with the user:")

print("session.delete(jack)")
session.delete(jack)

print("session.query(User).filter_by(name='jack').count(): ", session.query(User).filter_by(name='jack').count())

print("session.query(Address).filter(Address.email_address.in_(['jack@google.com', 'j25@yahoo.com'])).count(): ",
    session.query(Address).filter(Address.email_address.in_(['jack@google.com', 'j25@yahoo.com'])).count())

# Building a Many To Many Relationship
print("\n Building a Many To Many Relationship:")
