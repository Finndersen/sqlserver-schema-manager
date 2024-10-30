import pyodbc

from example.schema import server
from sssm.align import align_server

connectionString = "DRIVER={ODBC Driver 18 for SQL Server};SERVER=localhost;DATABASE=master;UID=SA;PWD==Strong@Passw0rd"

connection = pyodbc.connect(connectionString)


align_server(connection.cursor(), server)
