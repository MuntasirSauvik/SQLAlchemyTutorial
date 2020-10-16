import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey

# Version Check
print("sqlalchemy verion:"+ sqlalchemy.__version__  )

# Connecting , import statement at line 2 "from sqlalchemy import create_engine" is also needed.
engine = create_engine('sqlite:///:memory:', echo=True)


# Define and Create Tables, import statement at line 3 "from sqlalchemy import Table, Column, Integer, String, MetaData,
# ForeignKey" is also needed.
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
ins = users.insert()
print (str(ins))
ins = users.insert().values(name='jack', fullname='Jack Jones')
print (str(ins))

