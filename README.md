# SQLServer Schema Manager


## Introduction

A library for programmatically managing the schema of a SQLServer Database, with two modes of operation:
* Declare the entire structure of a SQLServer instance in code, and apply it to a target database (automatically determines and executes all diff changes required).
* Programmatically/interactively inspect the structure of an existing SQLServer instance, and make changes using code instead of SQL.

It can be used in tandem with other tools that manage a DB schema (e.g. an ORM), to manage the features that they do not support (e.g. the database itself, users, partitioning and compression).

Important note: This is a tool I developed for my own purposes many years ago, it has a lot of functionality and works quite well but is not recommended to be used in a production context without some further work. 

## Installation

This library is not packed or included in any package manager, so you will need to clone the repository and manually include it in your project. 

## Usage

### Declarative Mode

1. Define your desired server schema:

```python
from sssm.db_entities.declared import Server, Database, Table, Index, ForeignKey, Login, IntegerColumn, VarcharColumn, DateColumn, IdentityColumn, FloatColumn, User

# Currently does not support creating Logins, so these will need to correspond to ones already existing on the server
admin_login = Login('admin')
writer_login = Login('writer', server_roles=['bulkadmin'])
reader_login = Login('reader')

server = Server(
    databases=[
        Database(
            name='MyDatabase',
            owner=admin_login.name,
            data_file_dir=r'D:\MSSQL\DATA',
            log_file_dir=r'D:\MSSQL\LOG',
            data_size=100_000,
            # Can also specify schema name if desired, otherwise default "db" is used
            tables=[
                Table(
                    name='Person',
                    columns=[
                        IdentityColumn(name='ID', primary_key=True),
                        VarcharColumn(name='Name', char_max_len=255),
                        DateColumn(name='DateOfBirth'),
                        IntegerColumn(name='HeightCM'),
                        FloatColumn(name='WeightKG'),
                        IntegerColumn(name='AddressID'),
                    ],
                    indexes=[
                        Index(name='IX_MyTable_Name', columns=['Name'], compression='PAGE'),
                    ],
                    foreign_keys=[
                        ForeignKey(column='AddressID', foreign_table='Address', foreign_column='ID'),
                    ],
                ),
                Table(
                    name='Address',
                    columns=[
                        IdentityColumn(name='ID', primary_key=True),
                        IntegerColumn(name='StreetNumber'),
                        VarcharColumn(name='StreetName', char_max_len=255),
                        VarcharColumn(name='PostCode', char_max_len=6),
                        VarcharColumn(name='State', char_max_len=100),
                        VarcharColumn(name='Country', char_max_len=255),
                    ],
                ),
            ],
            users=[
                User.for_login(admin_login),                    
                User.for_login(writer_login, db_roles=['db_datawriter']),   
                User.for_login(reader_login, db_roles=['db_datareader']),
            ]
        )
    ],
    logins=[
        admin_login,
        writer_login,
        reader_login,
    ],
)
```

Apply the schema to a target database:

```python
import pyodbc

from sssm.align import align_server
from .schema import server

# Create connection using an admin login
connection = pyodbc.connect(...)

align_server(connection.cursor(), server)
```

This should be run from an interactive Python session or script, as it will prompt for confirmation before applying destructive changes.

### Inspection Mode

Inspecting an existing server schema can be done either programmatically or in an interactive shell:

```python
>>> import pyodbc
>>> from sssm.db_entities.reflected import ReflectedServer
# Create connection using an admin login
>>> connection = pyodbc.connect(...)
>>> server = ReflectedServer.from_cursor(connection.cursor())
>>> database = server.get_current_database()    # Get database specified in connection
>>> person_table = database.get_table('Person') # Get table by name
>>> person_table.get_compression()
'NONE'
>>> person_table.set_compression('PAGE')        # Rebuild table with PAGE compression
>>> person_table.get_compression()
'PAGE'
>>> person_table.clear_data()

```

## Features

### Supported Database Entities

Supports managing the following database entities and attributes:
* Server Logins (not creating)
  * Server roles 
* Databases
  * Owner 
  * Data and log file directories
  * Data size
  * Recovery model
* Schemas
* Tables
* Users
  * Login name
  * DB roles
* Columns
  * Data types
  * Max length
  * Nullable
  * Datetime/Numeric precision
* Indexes / Primary keys
  * Columns
  * Clustered
  * Unique
  * Included columns
  * Compression
* Foreign keys
  * Source and target columns 
* Partitions
  * Partition column 

### Other Features

* Use `ignore_extra_children` on a declared object if you want to ignore (not delete) any extra existing children when aligning it to a reflected object.
* Set `old_name` on a declared object to identify and rename the existing object when aligning it to a database.
