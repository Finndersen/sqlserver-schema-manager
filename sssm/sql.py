###########################################################################################
#       GENERAL
###########################################################################################

CDC_MAX_LSN = 'SELECT sys.fn_cdc_get_max_lsn()'

max_column_value = "SELECT MAX([{column}]) FROM [{schema}].[{table}]"

min_column_value = "SELECT MIN([{column}]) FROM [{schema}].[{table}]"

###########################################################################################
#           DATABASES
###########################################################################################
CREATE_DB = """
CREATE DATABASE [{db_name}]
 CONTAINMENT = NONE
 ON  PRIMARY 
( NAME = N'{db_name}', FILENAME = N'{data_file_path}' , SIZE = {data_size}MB , MAXSIZE = UNLIMITED, FILEGROWTH = 10%)
 LOG ON 
( NAME = N'{db_name}_log', FILENAME = N'{log_file_path}' , SIZE = {log_size}MB , MAXSIZE = UNLIMITED , FILEGROWTH =10%)"""

db_option_set = 'ALTER DATABASE [{db_name}] SET {option_name} {value}'

db_option_check = "SELECT 1 FROM sys.databases WHERE name = '{db_name}' AND {field} = '{value}'"

db_sizes = """
SELECT 
row_size_mb = CAST(SUM(CASE WHEN type_desc = 'ROWS' THEN size END) * 8. / 1024 AS DECIMAL(10,2)),
log_size_mb = CAST(SUM(CASE WHEN type_desc = 'LOG' THEN size END) * 8. / 1024 AS DECIMAL(10,2))
FROM sys.master_files WITH(NOWAIT)
WHERE database_id = DB_ID('{db_name}')"""

DB_EXISTS = "SELECT 1 FROM sys.databases WHERE name = '{db_name}'"

db_file_info = """
SELECT name,
CAST(size/128.0 AS INT) AS current_size_mb, 
CAST(FILEPROPERTY(name, 'SpaceUsed')/128.0 AS INT) AS used_space_mb,
physical_name
FROM sys.master_files
WHERE database_id = DB_ID('{db_name}') AND type_desc = '{file_type}' """

SHRINK_DB_FILE = "DBCC SHRINKFILE (N'{file_name}' , {size})"

GROW_DB_FILE = """
ALTER DATABASE {db_name}
MODIFY FILE (NAME = {file_name}, SIZE = {size}MB)"""

LIST_DATABASES = """
SELECT name, database_id, state_desc, recovery_model_desc, suser_sname(owner_sid) AS owner FROM master.sys.databases
"""

RENAME_DATABASE = "ALTER DATABASE {old_name}  MODIFY NAME = {new_name}"

DATABASE_DETAIL = LIST_DATABASES + " WHERE name = '{db_name}'"

set_db_owner = "EXEC sp_changedbowner '{db_owner}'"

log_file_usage = "SELECT * FROM sys.dm_db_log_space_usage"

CHANGE_DB_FILE_PATH = """
ALTER DATABASE {db_name}
MODIFY FILE (name='{file_name}', filename='{file_path}')
"""

SET_DB_OFFLINE = 'ALTER DATABASE {db_name} SET OFFLINE WITH ROLLBACK IMMEDIATE'
SET_DB_ONLINE = 'ALTER DATABASE {db_name} SET ONLINE'

DB_IN_HAG = "SELECT 1 FROM sys.dm_hadr_database_replica_states WHERE database_id = DB_ID('{db_name}')"
###########################################################################################
#       LOGINS
###########################################################################################
CREATE_LOGIN = "CREATE LOGIN [{login_name}] WITH PASSWORD='{password}'"

LOGIN_EXISTS = "SELECT 1 FROM master.dbo.syslogins WHERE name = '{login_name}'"

ALTER_SERVER_ROLE = "ALTER SERVER ROLE [{role}] {action} MEMBER [{login_name}]"

LOGIN_ROLES = """
SELECT role.name
FROM sys.{scope}_role_members  
JOIN sys.{scope}_principals AS role  
    ON sys.{scope}_role_members.role_principal_id = role.principal_id  
JOIN sys.{scope}_principals AS member  
    ON sys.{scope}_role_members.member_principal_id = member.principal_id
WHERE member.name = '{login_name}' {extra_conditions}"""

LIST_LOGINS = """
SELECT sl.name, sl.isntuser, sp.type_desc, sl.sysadmin,  sl.securityadmin, sl.serveradmin, sl.setupadmin, sl.processadmin, sl.diskadmin, sl.dbcreator, sl.bulkadmin
FROM master.dbo.syslogins sl 
JOIN sys.server_principals sp ON sp.sid = sl.sid 
WHERE sl.isntuser = 0 AND type_desc = 'SQL_LOGIN'
"""

LOGIN_DETAIL = LIST_LOGINS + "AND sl.name = '{name}'"

DELETE_LOGIN = "DROP LOGIN {login_name}"

###########################################################################################
#       USERS
###########################################################################################
CREATE_USER = "CREATE USER [{user_name}] FOR LOGIN [{login_name}] WITH DEFAULT_SCHEMA=[dbo]"

get_user_roles = """
SELECT 
DP1.name AS role_name,   
  DP2.name AS user_name   
 FROM sys.database_role_members AS DRM  
 RIGHT OUTER JOIN sys.database_principals AS DP1  ON DRM.role_principal_id = DP1.principal_id  
 LEFT OUTER JOIN sys.database_principals AS DP2  ON DRM.member_principal_id = DP2.principal_id  
WHERE DP1.type = 'R' AND DP2.name = '{user_name}'
"""

LIST_USERS = '''
SELECT dp.name as [name], dp.principal_id, sp.name as [login_name]
FROM sys.database_principals dp
JOIN sys.server_principals sp ON dp.sid=sp.sid
WHERE dp.type_desc = 'SQL_USER' '''

USER_DETAIL = LIST_USERS + " AND dp.name = '{user_name}'"

alter_db_role = "ALTER ROLE [{role}] {action} MEMBER [{user_name}]"

DROP_USER = "DROP USER {user_name}"

USER_FOR_LOGIN = LIST_USERS + " AND sp.name = '{login_name}'"

USER_EXISTS = "SELECT 1 FROM sys.database_principals WHERE type='S' AND name ='{user_name}'"

###########################################################################################
#           SCHEMAS
###########################################################################################
LIST_SCHEMAS = "SELECT name, schema_id, principal_id FROM [{db_name}].sys.schemas WHERE schema_id < 16384"

schema_detail= LIST_SCHEMAS + " AND name = '{schema_name}'"

CREATE_SCHEMA = "CREATE SCHEMA [{schema_name}]"

SCHEMA_EXISTS = "SELECT 1 FROM sys.schemas WHERE name='{schema}'"

###########################################################################################
#           TABLES
###########################################################################################
list_tables_in_schema = """
SELECT t.name, t.type_desc FROM sys.tables t
JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE s.name = '{schema}' """

table_details = list_tables_in_schema + " AND t.name = '{table}' "

table_compression_details = """
SELECT [s].name as [schema_name], [t].[name] AS [table_name], [p].[partition_number] AS [Partition],
    [p].[data_compression_desc] AS [compression]
FROM [{db_name}].[sys].[partitions] AS [p]
INNER JOIN [{db_name}].sys.tables AS [t] ON [t].[object_id] = [p].[object_id]
INNER JOIN [{db_name}].sys.schemas [s] ON s.schema_id = t.schema_id
WHERE [p].[index_id] in (0,1) AND [s].name = '{schema}' and [t].name = '{table_name}'
"""

CREATE_TABLE = "CREATE TABLE [{schema}].[{table}] ({columns}) ON [PRIMARY]"

DROP_TABLE = "DROP TABLE [{schema}].[{table}]"

#table_def ="SELECT COLUMN_NAME, DATA_TYPE, COALESCE(CHARACTER_MAXIMUM_LENGTH, DATETIME_PRECISION), IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}'" #, COLUMN_DEFAULT

table_has_data = "SELECT 1 FROM [{schema}].[{table}]"

DELETE_DATA = "DELETE FROM [{schema}].[{table}]"

TABLE_EXISTS = "SELECT 1 FROM [{db_name}].INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}'"


RENAME_TABLE = "exec sp_rename '{schema}.{old_name}', '{new_name}'"


set_identity_insert = "SET IDENTITY_INSERT [{db_name}].[{schema}].[{table}] {value}"

# Use the REBUILD WITH syntax to rebuild an entire table including all the partitions in a partitioned table.
# If the table has a clustered index, the REBUILD option rebuilds the clustered index. REBUILD can be run as an ONLINE operation.
SET_TABLE_COMPRESSION = "ALTER TABLE [{schema}].[{table}] REBUILD WITH (DATA_COMPRESSION = {compression}, ONLINE = {online})"

###########################################################################################
#           COLUMNS
###########################################################################################
column_is_primary_key = """
SELECT K.TABLE_NAME,
K.COLUMN_NAME,
K.CONSTRAINT_NAME
FROM [{db_name}].INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS C
JOIN [{db_name}].INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS K
ON C.TABLE_NAME = K.TABLE_NAME
AND C.CONSTRAINT_CATALOG = K.CONSTRAINT_CATALOG
AND C.CONSTRAINT_SCHEMA = K.CONSTRAINT_SCHEMA
AND C.CONSTRAINT_NAME = K.CONSTRAINT_NAME
WHERE c.CONSTRAINT_TYPE = 'PRIMARY KEY'
AND K.TABLE_SCHEMA = '{schema_name}'
AND K.COLUMN """

TABLE_COLUMNS = "SELECT name FROM sys.columns WHERE object_id = OBJECT_ID('{schema}.{table}') ORDER BY column_id"
COLUMN_EXISTS = "SELECT 1 FROM sys.columns WHERE object_id=OBJECT_ID('[{schema}].[{table}]') AND name = '{column_name}'"

# Get details of columns in table
column_detail_base = """
SELECT 
sc.name, 
isc.DATA_TYPE as [data_type], 
isc.CHARACTER_MAXIMUM_LENGTH as [char_max_len],
isc.DATETIME_PRECISION as [datetime_precision],
isc.NUMERIC_PRECISION as [numeric_precision],
isc.NUMERIC_SCALE as [numeric_scale], 
sc.is_nullable as [nullable], 
sc.is_identity as [identity]
FROM  sys.columns sc
	 JOIN sys.tables t ON t.object_id = sc.object_id 
	 JOIN sys.schemas s ON s.schema_id = t.schema_id
	 JOIN INFORMATION_SCHEMA.COLUMNS isc ON isc.TABLE_SCHEMA = s.name AND isc.TABLE_NAME = t.name AND isc.COLUMN_NAME=sc.name

WHERE s.name = '{schema}' AND t.name='{table}' """

table_column_details = column_detail_base + " ORDER BY sc.column_id"

column_detail = column_detail_base + " AND sc.name = '{column_name}'"

ALTER_COLUMN = "ALTER TABLE [{schema}].[{table}] ALTER COLUMN {column_def}"

DROP_COLUMN = "ALTER TABLE [{schema}].[{table}] DROP COLUMN [{column_name}]"

ADD_COLUMN = "ALTER TABLE [{schema}].[{table}] ADD {column_def}"

RENAME_COLUMN = "exec sp_rename '[{schema}].[{table}].[{old_name}]', '[{new_name}]', 'COLUMN'"

###########################################################################################
#           CONSTRAINTS
###########################################################################################
CREATE_CONSTRAINT = "ALTER TABLE [{schema}].[{table}] ADD CONSTRAINT {name} {type} ({constraint_fields})"


DROP_CONSTRAINT = "ALTER TABLE [{schema}].[{table}] DROP CONSTRAINT {constraint_name}"


TABLE_FOREIGN_KEYS = """
SELECT name, type  FROM  sys.foreign_keys
WHERE parent_object_id = OBJECT_ID('[{schema}].[{table}]')"""


CREATE_FOREIGN_KEY = """ALTER TABLE [{schema}].[{table}]
add constraint {fk_name} FOREIGN KEY ({column}) REFERENCES [{foreign_schema}].[{foreign_table}]({foreign_column}) """


# Find foreign keys ON other tables that reference this table
table_referencing_foreign_keys = """
SELECT
    OBJECT_SCHEMA_NAME(fkc.parent_object_id) as 'referencing_table_schema',
    OBJECT_NAME(fkc.parent_object_id) as 'referencing_table_name',
    COL_NAME(fkc.parent_object_id, fkc.parent_column_id) as 'referencing_column_name',
    COL_NAME(fkc.referenced_object_id, fkc.referenced_column_id) as 'referenced_column_name',
    OBJECT_NAME(fkc.constraint_object_id) 'constraint_name'	
FROM sys.foreign_key_columns fkc
JOIN sys.objects obj ON fkc.referenced_object_id = obj.object_id
WHERE OBJECT_SCHEMA_NAME(fkc.referenced_object_id) = '{schema}' AND  OBJECT_NAME(fkc.referenced_object_id) = '{table}' """

# Find foreign keys ON current table

FK_DETAILS = """
SELECT
	OBJECT_SCHEMA_NAME(fkc.referenced_object_id) as 'foreign_schema',
	OBJECT_NAME(fkc.referenced_object_id) as 'foreign_table',
	COL_NAME(fkc.referenced_object_id, fkc.referenced_column_id) as 'foreign_column',
    COL_NAME(fkc.parent_object_id, fkc.parent_column_id) as 'column',
    fk.name as 'constraint_name'	
FROM sys.foreign_key_columns fkc
JOIN sys.foreign_keys fk ON fk.object_id = fkc.constraint_object_id
WHERE fkc.parent_object_id = OBJECT_ID('[{schema}].[{table}]') AND fk.name = '{fk_name}' """


###########################################################################################
#           INDEXES
###########################################################################################

indexes_on_column = """
    SELECT ind.name AS name, '' AS compression, ind.type_desc AS clustering, obj.type AS type, ind.is_unique_constraint AS is_constraint
    FROM    sys.indexes ind
        INNER JOIN sys.index_columns ic ON ind.object_id = ic.object_id AND ind.index_id = ic.index_id
        INNER JOIN sys.columns col      ON ic.object_id = col.object_id AND ic.column_id = col.column_id
        INNER JOIN sys.tables t         ON ind.object_id = t.object_id
        LEFT OUTER JOIN sys.objects obj ON obj.name = ind.name
    WHERE   ind.object_id = OBJECT_ID('{schema}.{table}') AND ic.key_ordinal != 0 AND col.name = '{column_name}' """

table_index_details_old= """SELECT DISTINCT SI.name AS name, SP.data_compression_desc AS compression, SI.type_desc AS clustering, SO.type AS type, SI.is_unique_constraint AS is_constraint
    FROM sys.partitions SP 
    INNER JOIN sys.tables ST ON st.object_id = sp.object_id 
    INNER JOIN sys.indexes SI ON SI.object_id = sp.object_id AND SI.index_id = sp.index_id
    LEFT OUTER JOIN sys.objects SO ON SO.name = si.name
    WHERE SI.type_desc != 'HEAP' AND sp.object_id = OBJECT_ID('{schema}.{table}') """

table_index_details = """
SELECT 
     ind.name as [index_name],
     ind.index_id as [index_id],
	 ind.type_desc as [type_desc],
	 ind.is_primary_key,
	 ind.is_unique as [unique],
	 ind.is_unique_constraint AS is_unique_constraint,
	 sp.data_compression_desc as [compression]
FROM 
     sys.indexes ind 
INNER JOIN sys.partitions sp ON sp.object_id = ind.object_id AND sp.index_id = ind.index_id

WHERE      
    ind.object_id = OBJECT_ID('[{schema}].[{table}]') """

named_index_details = table_index_details + " AND ind.name = '{index_name}' "


index_columns_base = """
SELECT 
     ind.name as index_name,
     ind.index_id as index_id,
     ic.index_column_id as index_column_id,
     col.name as column_name
FROM 
     sys.indexes ind 
INNER JOIN 
     sys.index_columns ic ON  ind.object_id = ic.object_id AND ind.index_id = ic.index_id 
INNER JOIN 
     sys.columns col ON ic.object_id = col.object_id AND ic.column_id = col.column_id 
WHERE      
      ind.name='{index_name}'
      AND ind.object_id = OBJECT_ID('[{schema}].[{table}]') """

index_columns_order = " ORDER BY ind.index_id, ic.key_ordinal "

index_nonpartition_columns = index_columns_base + " AND ic.is_included_column = 0 AND ic.partition_ordinal = 0 " + index_columns_order

index_all_columns = index_columns_base + " AND ic.is_included_column = 0 " + index_columns_order

index_included_columns = index_columns_base + " AND ic.is_included_column = 1 " + index_columns_order

INDEX_EXISTS = "SELECT 1 FROM sys.indexes WHERE name='{index_name}' AND object_id=OBJECT_ID('{schema}.{table}')"

TABLE_INDEX_NAMES = "SELECT name FROM sys.indexes WHERE is_primary_key=0 AND type IN (1,2) AND object_id=OBJECT_ID('{schema}.{table}')"

RENAME_INDEX = "exec sp_rename '[{schema}].[{table}].[{old_name}]', '{new_name}', 'INDEX'"

CREATE_INDEX = """
CREATE {unique} {clustering} INDEX [{name}] ON [{schema}].[{table}] ({columns})
    {include} WITH (DATA_COMPRESSION = {compression}, DROP_EXISTING={drop_existing}) ON {create_on}"""

DROP_INDEX = "DROP INDEX [{index_name}] ON [{schema}].[{table}]"


index_frag_stats = """SELECT ind_stat.partition_number, ind.name, ind_stat.avg_fragmentation_in_percent FROM sys.dm_db_index_physical_stats(DB_ID(N'{db_name}'), OBJECT_ID(N'{schema}.{table}'), NULL,NULL, NULL) AS ind_stat 
    LEFT OUTER JOIN sys.indexes AS ind ON ind_stat.object_id = ind.object_id AND ind_stat.index_id = ind.index_id
    WHERE ind_stat.index_id != 0"""

ALTER_INDEX = "ALTER INDEX [{index_name}] ON [{schema}].[{table}] REBUILD PARTITION = ALL WITH (DATA_COMPRESSION = {compression}, ONLINE = {online})"

###########################################################################################
#           PRIMARY KEYS
###########################################################################################
CREATE_PK = """
    ALTER TABLE [{schema}].[{table}] ADD CONSTRAINT {pk_name} 
    PRIMARY KEY {clustering} ({columns}) 
    WITH (DATA_COMPRESSION = {compression})
"""

PK_EXISTS = INDEX_EXISTS + ' AND is_primary_key=1'

TABLE_PK_COLUMNS = """
SELECT
    kc.name as pk_name,
    c.NAME as column_name
FROM 
    sys.key_constraints kc
INNER JOIN sys.index_columns ic ON kc.parent_object_id = ic.object_id  AND kc.unique_index_id = ic.index_id
INNER JOIN  sys.columns c  ON ic.object_id = c.object_id AND ic.column_id = c.column_id
WHERE
kc.[type] = 'PK' AND
c.object_id = OBJECT_ID('[{schema_name}].[{table_name}]')
ORDER BY ic.key_ordinal"""

TABLE_PK_NAME = "SELECT name FROM sys.indexes WHERE is_primary_key=1 AND object_id=OBJECT_ID('{schema}.{table}')"
###########################################################################################
#           COMPRESSION
###########################################################################################
#Compression
compression_check="""
    SELECT  SP.partition_number, SP.data_compression_desc
    FROM sys.partitions SP 
    INNER JOIN sys.tables ST ON st.object_id = sp.object_id 
    INNER JOIN sys.indexes SI ON SI.object_id = sp.object_id AND SI.index_id = sp.index_id
    WHERE st.name = '{table}' AND SI.type <= 1 AND SP.data_compression_desc != '{compression}' """


###########################################################################################
#           PARTITIONS
###########################################################################################
#Check if specified table has any partition schemes applied
"""
    SELECT 1
    FROM sys.tables AS t
    JOIN sys.indexes AS i
        ON t.[object_id] = i.[object_id]
        AND i.[type] IN (0,1)
    JOIN sys.partition_schemes ps
        ON i.data_space_id = ps.data_space_id
    JOIN sys.schemas s
        ON t.schema_id = s.schema_id
    WHERE t.name = '{table}' AND s.name = '{schema}' """

# Get list of partition function details including name, type and scale of parameter
partition_function_detail = """
    SELECT pf.name, pp.scale as type_scale, st.name AS type_name
    FROM sys.partition_functions pf 
    JOIN sys.partition_parameters pp ON pf.function_id = pp.function_id
    JOIN sys.types st on st.system_type_id = pp.system_type_id
"""

table_partition_names = """
    SELECT ps.name as ps_name, pf.name as pf_name
    FROM sys.indexes i 
    JOIN sys.partition_schemes ps ON ps.data_space_id = i.data_space_id 
    JOIN sys.partition_functions pf ON pf.function_id = ps.function_id
    WHERE i.object_id = object_id('{schema}.{table}') AND i.type IN (0,1)
"""

partition_name_exists_on_table = table_partition_names + " AND ps.name = '{ps_name}'"

partition_function_for_scheme = """
    SELECT pf.name as name
    FROM sys.partition_functions pf
    JOIN sys.partition_schemes ps on ps.function_id = pf.function_id
    WHERE ps.name = '{ps_name}'
"""

check_index_partition = """
    SELECT ps.name, pf.name
    FROM sys.indexes i 
    JOIN sys.partition_schemes ps ON ps.data_space_id = i.data_space_id 
    JOIN sys.partition_functions pf ON pf.function_id = ps.function_id
    WHERE i.object_id = object_id('{schema}.{table}') AND i.name = '{index_name}'
"""

CREATE_PARTITION_FUNCTION = """
    CREATE PARTITION FUNCTION {pf_name}  ({column_type})
    AS RANGE RIGHT FOR VALUES ({boundary_values});
"""

CREATE_PARTITION_SCHEME = """
    CREATE PARTITION SCHEME {ps_name}
    AS PARTITION {pf_name} 
    ALL TO ([PRIMARY]);
"""

TABLE_PARTITION_DETAILS= """
    SELECT c.name AS column_name, ps.name as ps_name, pf.name as pf_name
    FROM  sys.tables          t
    JOIN  sys.indexes         i 
          ON (i.object_id = t.object_id )
    JOIN  sys.index_columns  ic 
          ON (ic.index_id = i.index_id AND ic.object_id = t.object_id)
    JOIN  sys.columns         c 
          ON (c.object_id = ic.object_id AND c.column_id = ic.column_id)
    JOIN sys.partition_schemes ps ON ps.data_space_id = i.data_space_id 
    JOIN sys.partition_functions pf ON pf.function_id = ps.function_id
    WHERE t.object_id  = object_id('{schema}.{table}') AND ic.partition_ordinal > 0  AND i.index_id < 2
"""

table_partition_details_for_column = TABLE_PARTITION_DETAILS + " AND c.name = '{column_name}'"

table_partition_details_for_scheme = TABLE_PARTITION_DETAILS + " AND ps.name = '{ps_name}'"

DROP_PARTITION_FUNCTION = "DROP PARTITION FUNCTION {pf_name}"

DROP_PARTITION_SCHEME = "DROP PARTITION SCHEME {ps_name}"

PARITION_NUMBER_FOR_VALUE = "SELECT $PARTITION.{pf_name}('{value}') AS number"


PARITION_SCHEME_EXISTS = "SELECT 1 FROM sys.partition_schemes WHERE name = '{ps_name}'"
PARTITION_FUNCTION_EXISTS = "SELECT 1 FROM sys.partition_functions WHERE name = '{pf_name}'"

PARTITION_RANGE_VALUES = """
    SELECT CAST(sprv.value AS [datetime2](0)) as value
    FROM sys.partition_functions spf
    JOIN sys.partition_schemes sps ON spf.function_id = sps.function_id
    INNER JOIN sys.partition_range_values sprv ON sprv.function_id=spf.function_id
    WHERE (sps.name='{ps_name}')
    ORDER BY sprv.boundary_id ASC"""

MERGE_PARTITION_RANGE = "ALTER PARTITION FUNCTION {fn_name}() MERGE RANGE ('{merge_date}')"

SET_PARTITION_NEXT_FILEGROUP = "ALTER PARTITION SCHEME [{scheme_name}]  NEXT USED [PRIMARY]"

SPLIT_PARITION_RANGE = "ALTER PARTITION FUNCTION {fn_name}() SPLIT RANGE ('{new_date}')"


TABLE_ROW_COUNT = """
    SELECT SUM(PART.rows) AS rows
    FROM sys.tables TBL
    INNER JOIN sys.partitions PART ON TBL.object_id = PART.object_id
    INNER JOIN sys.indexes IDX ON PART.object_id = IDX.object_id
    AND PART.index_id = IDX.index_id
    WHERE TBL.name = '{table}'
    AND IDX.index_id < 2
    GROUP BY TBL.object_id, TBL.name"""



SELECT = "SELECT {fields} FROM [{schema}].[{table}] {condition} {group_by} {order_by}"


TRUNCATE_TABLE_PARTITIONS = "TRUNCATE TABLE [{schema}].[{table}] WITH (PARTITIONS ({start_partition} TO {end_partition}))"