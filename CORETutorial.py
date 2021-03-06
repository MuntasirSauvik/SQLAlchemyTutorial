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
from sqlalchemy.dialects.oracle import dialect as OracleDialect
from sqlalchemy.sql import bindparam
from sqlalchemy.sql import func
from sqlalchemy.sql import column

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

# Using Aliases and Subqueries
print("\nUsing Aliases and Subqueries")
a1 = addresses.alias()
a2 = addresses.alias()
s = select([users]).where(
    and_(
        users.c.id == a1.c.user_id,
        users.c.id == a2.c.user_id,
        a1.c.email_address == 'jack@msn.com',
        a2.c.email_address == 'jack@yahoo.com'
    ))
print(conn.execute(s).fetchall())

# For the purposes of debugging, it can be specified by passing a string name to the FromClause.alias() method:
print("\nFor the purposes of debugging, it can be specified by passing a string name to the FromClause.alias() method:")
a1 = addresses.alias('a1')
address_subq = s.alias()
s = select([users.c.name]).where(users.c.id == address_subq.c.id)
print(conn.execute(s).fetchall())

# Using Joins
print("\nUsing Joins")
print(users.join(addresses))

# if we want to join on all users who use the same name in their email address as their username:
print("\nif we want to join on all users who use the same name in their email address as their username:")
print(users.join(addresses, addresses.c.email_address.like(users.c.name + '%')))

s = select([users.c.fullname]).select_from(
    users.join(addresses,
    addresses.c.email_address.like(users.c.name + '%'))
)
print(conn.execute(s).fetchall())

# The FromClause.outerjoin() method creates LEFT OUTER JOIN constructs, and is used in the same way as FromClause.join():
print("\nThe FromClause.outerjoin() method creates LEFT OUTER JOIN constructs, and is used in the same way as "
      "FromClause.join():")
s = select([users.c.fullname]).select_from(users.outerjoin(addresses))
print(s)

#Oracle-specific SQL:
print("\nOracle-specific SQL:")
print(s.compile(dialect=OracleDialect(use_ansi=False)))


# Common Table Expressions (CTE)
print("\nCommon Table Expressions (CTE)")
users_cte = select([users.c.id, users.c.name]).where(users.c.name == 'wendy').cte()
stmt = select([addresses]).where(addresses.c.user_id == users_cte.c.id).order_by(addresses.c.id)
print(conn.execute(stmt).fetchall())

# The RECURSIVE format of CTE
print("\nThe RECURSIVE format of CTE")
users_cte = select([users.c.id, users.c.name]).cte(recursive=True)
users_recursive = users_cte.alias()
users_cte = users_cte.union(select([users.c.id, users.c.name]).where(users.c.id > users_recursive.c.id))
stmt = select([addresses]).where(addresses.c.user_id == users_cte.c.id).order_by(addresses.c.id)
print(conn.execute(stmt).fetchall())

# Everything Else
print("\nEverything Else")

# Bind Parameter Objects
print("\nBind Parameter Objects")
s = users.select(users.c.name == bindparam('username'))
print(conn.execute(s, username='wendy').fetchall())


# Another important aspect of bindparam() is that it may be assigned a type.
print("\nAnother important aspect of bindparam() is that it may be assigned a type. ")
s = users.select(users.c.name.like(bindparam('username', type_=String) + text("'%'")))
print(conn.execute(s, username='wendy').fetchall())

# bindparam() constructs of the same name can also be used multiple times, where only a single named value is needed in the execute parameters:
print("\n bindparam() constructs of the same name can also be used multiple times, where only a single named value is "
      "needed in the execute parameters:")
s = select([users, addresses]).where(
    or_(
        users.c.name.like(
        bindparam('name', type_=String) + text("'%'")),
        addresses.c.email_address.like(
        bindparam('name', type_=String) + text("'@%'"))
        )
    ).select_from(users.outerjoin(addresses)).order_by(addresses.c.id)
print(conn.execute(s, name='jack').fetchall()
      )

# Functions
print("\nFunctions")
print(func.now())
print(func.concat('x', 'y'))
print(func.xyz_my_goofy_function())

print("Some functions are know by SQLAlchemy thus they don't get the parenthesis added after them")
print(func.current_timestamp())

# Below, we use the result function scalar() to just read the first column of the first row and then close the result
print("\nBelow, we use the result function scalar() to just read the first column of the first row and then close the result")
print(
    conn.execute(
        select([
            func.max(addresses.c.email_address, type_=String).
            label('maxemail')
        ])
    ).scalar()
)

# we can construct using “lexical” column objects as well as bind parameters:
print("\nwe can construct using “lexical” column objects as well as bind parameters:")
calculate = select([column('q'), column('z'), column('r')]).select_from(
                func.calculate(
                    bindparam('x'),
                    bindparam('y')
                )
            )
calc = calculate.alias()
print(select([users]).where(users.c.id > calc.c.z))

# If we wanted to use our calculate statement twice with different bind parameters, the unique_params() function will
# create copies for us, and mark the bind parameters as “unique” so that conflicting names are isolated.
print("\nIf we wanted to use our calculate statement twice with different bind parameters, the unique_params() function "
      "will create copies for us, and mark the bind parameters as “unique” so that conflicting names are isolated. ")
calc1 = calculate.alias('c1').unique_params(x=17, y=45)
calc2 = calculate.alias('c2').unique_params(x=5, y=12)
s = select([users]).where(
    users.c.id.between(calc1.c.z, calc2.c.z))

print(s)
print(s.compile().params)

# Window Functions
print("\nWindow Functions")

s = select([
        users.c.id,
        func.row_number().over(order_by=users.c.name)
    ])
print(s)

# FunctionElement.over() also supports range specification using either the over.rows or over.range parameters:
print("\nFunctionElement.over() also supports range specification using either the over.rows or over.range parameters:")
s = select([
        users.c.id,
        func.row_number().over(
            order_by=users.c.name,
            rows=(-2, None)
        )
    ])
print(s)

#
print("\n")


#
print("\n")

#
print("\n")