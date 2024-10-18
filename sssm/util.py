
INT_TYPES = {'bigint', 'int', 'smallint', 'tinyint'}    # No parameters
NUMERIC_TYPES = {'decimal', 'numeric'}                  # 1 or 2 parameters (precision and scale)
MONEY_TYPES = {'money', 'smallmoney'}                   # No parameters
APPROX_NUMBER_TYPES = {'float', 'real'}                 # 1 parameter (precision)
DATETIME_NO_PRECISION_TYPES = {'date', 'datetime', 'smalldatetime'}  # No parameters
DATETIME_PRECISION_TYPES = {'time', 'datetime2', 'datetimeoffset'}  # 1 parameter (datetime precision)
CHAR_TYPES = {'char', 'varchar', 'nchar', 'nvarchar', 'binary', 'varbinary'}


def column_sql_representation(name, data_type, identity=False, nullable=False, char_max_len=None,
                              datetime_precision=7, numeric_precision=None, numeric_scale=None):
    """
    Get SQL Create representation of column with specific attributes
    :param str name:
    :param str data_type:
    :param bool identity: Whether column is identity
    :param bool nullable: Whether column is nullable
    :param int char_max_len: Max length of char types
    :param int datetime_precision: Precision of datetime types
    :param int numeric_precision: Precision of numeric types
    :param int numeric_scale: Scale of numeric types
    :return:
    """
    sql_components = [name]

    sql_components.append(data_type_representation(data_type, char_max_len=char_max_len,
                                                   datetime_precision=datetime_precision,
                                                   numeric_precision=numeric_precision,
                                                   numeric_scale=numeric_scale))

    if identity:
        sql_components.append('IDENTITY(1,1)')

    if nullable:
        sql_components.append('NULL')
    else:
        sql_components.append('NOT NULL')

    return ' '.join(sql_components)


def data_type_representation(data_type, char_max_len=None, datetime_precision=7, numeric_precision=None, numeric_scale=None):
    if data_type in INT_TYPES | MONEY_TYPES | DATETIME_NO_PRECISION_TYPES:
        return data_type
    elif data_type in NUMERIC_TYPES | APPROX_NUMBER_TYPES:
        if numeric_precision is None:
            raise ValueError('Numeric precision must be provided for types: {}'.format(NUMERIC_TYPES | APPROX_NUMBER_TYPES))
        if data_type in NUMERIC_TYPES and numeric_scale is not None:
            return '{}({},{})'.format(data_type, numeric_precision, numeric_scale)
        else:
            return '{}({})'.format(data_type, numeric_precision)
    elif data_type in DATETIME_PRECISION_TYPES:
        return '{}({})'.format(data_type, datetime_precision)
    elif data_type in CHAR_TYPES:
        if char_max_len is None:
            raise ValueError('Char length must be provided for types: {}'.format(CHAR_TYPES))
        return '{}({})'.format(data_type, char_max_len)
    else:
        raise ValueError('Unsupported column type: "{}"'.format(data_type))


class AutoCommit(object):
    """
    Context manager class which enables autocommit on database connection
    """
    def __init__(self, conn):
        """

        :param conn: databse connection
        """
        self.conn = conn

    def __enter__(self):
        self.conn.commit()
        self.conn.autocommit = True
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.conn.autocommit = False
