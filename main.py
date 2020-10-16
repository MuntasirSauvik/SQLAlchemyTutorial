import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey
from sqlalchemy.sql import select
from sqlalchemy import type_coerce
from sqlalchemy.sql import and_, or_, not_
from sqlalchemy.sql import text
from sqlalchemy import select, and_, text, String
from sqlalchemy.sql import table, literal_column
from sqlalchemy import func
from sqlalchemy import func, desc

# Version Check
print("Version Check")
print("sqlalchemy verion:"+ sqlalchemy.__version__  )

# Connecting , import statement at line 2 "from sqlalchemy import create_engine" is also needed.
engine = create_engine('sqlite:///:memory:', echo=True)

# Define and Create Tables, import statement at line 3 "from sqlalchemy import Table, Column, Integer, String, MetaData,
# ForeignKey" is also needed.
print("\nDefine and Create Tables")
metadata = MetaData()
users = Table('users', metadata,
        Column('id', Integer, primary_key=True),
        Column('name', String),
        Column('fullname', String),)

addresses = Table('addresses', metadata,
            Column('id', Integer, primary_key=True),
            Column('user_id', None, ForeignKey('users.id')),
            Column('email_address', String, nullable=False))

# tell the MetaData we’d actually like to create our selection of tables for real inside the SQLite database
metadata.create_all(engine)

# Insert Expressions
print("\n Insert Expressions")
ins = users.insert()
print(str(ins))
ins = users.insert().values(name='jack', fullname='Jack Jones')
print(str(ins))

# Executing
print("\n Executing")
conn = engine.connect()
print(conn)
result = conn.execute(ins)
print(result)

ins.bind = engine
print(str(ins))
print(result.inserted_primary_key)

# Executing Multiple Statements
print("\nExecuting Multiple Statements")
ins = users.insert()
print (conn.execute(ins, id=2, name='wendy', fullname='Wendy Williams'))

# Issue many inserts using DBAPI’s executemany() method
print("\nTo issue many inserts using DBAPI’s executemany() method,")
# result = conn.execute(addresses.insert(), [
#         {'user_id': 1, 'email_address' : 'jack@yahoo.com'},
#         {'user_id': 1, 'email_address' : 'jack@msn.com'},
#         {'user_id': 2, 'email_address' : 'www@www.org'},
#         {'user_id': 2, 'email_address' : 'wendy@aol.com'},
#     ])
# print(result)
print(
    conn.execute(addresses.insert(), [
        {'user_id': 1, 'email_address' : 'jack@yahoo.com'},
        {'user_id': 1, 'email_address' : 'jack@msn.com'},
        {'user_id': 2, 'email_address' : 'www@www.org'},
        {'user_id': 2, 'email_address' : 'wendy@aol.com'},
    ])
)
# The “executemany” style of invocation is available for each of the insert(), update() and delete() constructs.

# Selecting, import statement at line 4 "from sqlalchemy.sql import select" also needed for this step
print("\nSelecting")
s = select([users])
result = conn.execute(s)

# The result object can be iterated directly in order to provide an iterator of RowProxy objects:
print("\nThe result object can be iterated directly in order to provide an iterator of RowProxy objects:")
for row in result:
    print(row)

# methods of retrieving the data in each column. e.g. using the string names of columns:
print("\nMethods of retrieving the data in each column. e.g. using the string names of columns:")
result = conn.execute(s)
row = result.fetchone()
print("name:", row['name'], "; fullname:", row['fullname'])

# using integer indexes:
print("\nusing integer indexes:")
row = result.fetchone()
print("name:", row[1], "; fullname:", row[2])

# use the Column objects selected in our SELECT directly as keys:
print("\nuse the Column objects selected in our SELECT directly as keys:")
for row in conn.execute(s):
    print("name:", row[users.c.name], "; fullname:", row[users.c.fullname])

# ResultProxy is to be discarded before an auto-close occurs, it can be explicitly closed
result.close()

# Selecting Specific Columns
print("\nSelecting Specific Columns")
s = select([users.c.name, users.c.fullname])
result = conn.execute(s)
for row in result:
    print(row)

# Let’s try putting two tables into our select() statement:
print("\nLet’s try putting two tables into our select() statement:")
for row in conn.execute(select([users, addresses])):
    print(row)

# to put some sanity into this statement, we need a WHERE clause. We do that using Select.where():
print("\nto put some sanity into this statement, we need a WHERE clause. We do that using Select.where():")
s = select([users, addresses]).where(users.c.id == addresses.c.user_id)
for row in conn.execute(s):
    print(row)

# WHERE clause. So lets see exactly what that expression is doing:
print("\nWHERE clause. So lets see exactly what that expression is doing:")
print(str(users.c.id == addresses.c.user_id))

# Operators
print("\nOperators")
print(users.c.id == addresses.c.user_id)
print(users.c.id == 7)
print((users.c.id == 7).compile().params)
print(users.c.id != 7)
# None converts to IS NULL
print(users.c.name == None)
# reverse works too
print('fred' > users.c.name)
print(users.c.id + addresses.c.id)
print(users.c.name + users.c.fullname)
print(users.c.name.op('tiddlywinks')('foo'))

# Conjunctions
print("\nConjunctions")
print(and_(
        users.c.name.like('j%'),
        users.c.id == addresses.c.user_id,
        or_(
            addresses.c.email_address == 'wendy@aol.com',
            addresses.c.email_address == 'jack@yahoo.com'
        ),
        not_(users.c.id > 5)
        )
)

# can also use the re-jiggered bitwise AND, OR and NOT operators
print("\ncan also use the re-jiggered bitwise AND, OR and NOT operators:")
print(
    users.c.name.like('j%') & (users.c.id == addresses.c.user_id) &
    (
        (addresses.c.email_address == 'wendy@aol.com') | \
        (addresses.c.email_address == 'jack@yahoo.com')
    ) \
    & ~(users.c.id>5)
)

s = select([(users.c.fullname + ", " + addresses.c.email_address).label('title')]).where(
        and_(
            users.c.id == addresses.c.user_id,
            users.c.name.between('m', 'z'),
            or_(
                addresses.c.email_address.like('%@aol.com'),
                addresses.c.email_address.like('%@msn.com')
            )
        )
    )
print(conn.execute(s).fetchall())

# A shortcut to using and_() is to chain together multiple Select.where() clauses.
print("\nA shortcut to using and_() is to chain together multiple Select.where() clauses. ")
s = select([(users.c.fullname + ", " + addresses.c.email_address).label('title')]).where(
            users.c.id == addresses.c.user_id).where(
            users.c.name.between('m', 'z')).where(

                or_(
                    addresses.c.email_address.like('%@aol.com'),
                    addresses.c.email_address.like('%@msn.com')
                )
            )
print(conn.execute(s).fetchall())

# Using Textual SQL

s = text("SELECT users.fullname || ', ' || addresses.email_address AS title "
         "FROM users, addresses "
         "WHERE users.id = addresses.user_id "
         "AND users.name BETWEEN :x AND :y "
         "AND (addresses.email_address LIKE :e1 "
         "OR addresses.email_address LIKE :e2)")

print(conn.execute(s, x='m', y='z', e1='%@aol.com', e2='%@msn.com').fetchall())

# Specifying Bound Parameter Behaviors
print("\nSpecifying Bound Parameter Behaviors")
stmt = text("SELECT * FROM users WHERE users.name BETWEEN :x AND :y")
stmt = stmt.bindparams(x="m", y="z")
print(stmt)

# Specifying Result-Column Behaviors
print("\nSpecifying Result-Column Behaviors")
stmt = text("SELECT users.id, addresses.id, users.id, "
    "users.name, addresses.email_address AS email "
    "FROM users JOIN addresses ON users.id=addresses.user_id "
    "WHERE users.id = 1").columns(
    users.c.id,
    addresses.c.id,
    addresses.c.user_id,
    users.c.name,
    addresses.c.email_address
)
result = conn.execute(stmt)
print(result)
row = result.fetchone()
print(row)
print(row[addresses.c.email_address])
# row["id"]

# Using text() fragments inside bigger statements
print("\nUsing text() fragments inside bigger statements")

s = select([
    text("users.fullname || ', ' || addresses.email_address AS title")]).where(
    and_(
        text("users.id = addresses.user_id"),
        text("users.name BETWEEN 'm' AND 'z'"),
        text(
            "(addresses.email_address LIKE :x "
            "OR addresses.email_address LIKE :y)")
    )
        ).select_from(text('users, addresses'))
print(conn.execute(s, x='%@aol.com', y='%@msn.com').fetchall())

# Using More Specific Text with table(), literal_column(), and column()
print("Using More Specific Text with table(), literal_column(), and column()")


s = select([literal_column("users.fullname", String) +', ' + literal_column("addresses.email_address").label("title")
    ]).where(
            and_(
                literal_column("users.id") == literal_column("addresses.user_id"),
                text("users.name BETWEEN 'm' AND 'z'"),
                text(
                    "(addresses.email_address LIKE :x OR "
                    "addresses.email_address LIKE :y)")
            )
    ).select_from(table('users')).select_from(table('addresses'))

print(conn.execute(s, x='%@aol.com', y='%@msn.com').fetchall())

# Ordering or Grouping by a Label
print("\nOrdering or Grouping by a Label")

stmt = select([addresses.c.user_id,
        func.count(addresses.c.id).label('num_addresses')]).group_by("user_id").order_by("user_id", "num_addresses")
print(conn.execute(stmt).fetchall())

# We can use modifiers like asc() or desc() by passing the string name:
print("\nWe can use modifiers like asc() or desc() by passing the string name:")

stmt = select([
    addresses.c.user_id,
    func.count(addresses.c.id).label('num_addresses')]).\
    group_by("user_id").order_by("user_id", desc("num_addresses"))

print(conn.execute(stmt).fetchall())

# Below, we illustrate how using the ColumnElement eliminates ambiguity when we want to order by a column name that appears more than once:
print("\nBelow, we illustrate how using the ColumnElement eliminates ambiguity when we want to order by a column name that appears more than once:")

u1a, u1b = users.alias(), users.alias()
stmt = select([u1a, u1b]).where(u1a.c.name > u1b.c.name).order_by(u1a.c.name)
# using "name" here would be ambiguous

print(conn.execute(stmt).fetchall())