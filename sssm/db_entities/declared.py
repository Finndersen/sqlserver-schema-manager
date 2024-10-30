from sssm import exceptions
from sssm.db_entities import attributes, reflected
from sssm.util import column_sql_representation, data_type_representation
import os


class DeclaredEntity(object):
    # Tuple of child types associated with this DB entity
    child_types = tuple()
    object_type = None
    attribute_defaults = {}

    def __init__(self, name, old_name=None, ignore_extra_children=None, **kwargs):
        """
        :param str name: name of DB object which identifies it uniquely within parent
        :param str old_name: old name of DB object (used for renaming)
        :param list, bool ignore_extra_children: List of children object types to not delete if they are not delcared,
        or True for all children
        :param kwargs: consists of children and attributes
        :return:
        """
        if name is None:
            raise exceptions.DBError('Declared DB entity must be defined with a name')
        self.name = name[:128]  # Limit name to 128 characters
        self.old_name = old_name
        self.ignore_extra_children = (set(ignore_extra_children)
                                      if isinstance(ignore_extra_children, (list, tuple))
                                      else ignore_extra_children)
        self.children = {}
        for child_type in self.child_types:
            declared_children = kwargs.pop(child_type.object_type, None)
            if declared_children is not None:
                # Add children defined in initialisation
                if not isinstance(declared_children, (list, tuple)):
                    declared_children = [declared_children]

                for declared_child in declared_children:
                    self.add_child(declared_child)

        self.valid_attributes = attributes.valid_attributes[self.object_type]
        for attr_name in self.valid_attributes:
            setattr(self,attr_name,kwargs.pop(attr_name))
        # Raise error if any additional KWARGS provided (other than children and attributes)
        if kwargs:
            raise ValueError('Unexpected provided DB entity attributes: {}'.format(kwargs))

    def ignore_extra_children_type(self, child_type):
        """
        Whether to ignore any extra existing database entity children of given type
        :param child_type:
        :return:
        """
        if isinstance(self.ignore_extra_children, set):
            return child_type in self.ignore_extra_children
        else:
            return bool(self.ignore_extra_children)

    def add_child(self, declared_child):
        # Check if child type is valid
        if not any(isinstance(declared_child, child_type) for child_type in self.child_types):
            raise exceptions.InvalidDBEntityChildError(
                'DB Entity: "{}" cannot add child of type: "{}"'.format(type(self).__name__,
                                                                        type(declared_child).__name__))
        # Initialise empty dictionary for children if not exists
        if declared_child.object_type not in self.children:
            self.children[declared_child.object_type] = []
        # Add instance to child list
        self.children[declared_child.object_type].append(declared_child)
        return self

    def get_children(self, child_type):
        return self.children.get(child_type,None)

    def get_child(self, child_type, child_name):
        for child in self.get_children(child_type):
            if child.name == child_name:
                return child
        else:
            raise exceptions.InvalidDBEntityChildError('Could not find child of type: {} with name: {}'.format(child_type, child_name))

    def get_object_by_id(self, *object_names):
        """
        Get DB object from chain of object names relative to current object
        :param str object_names: reference to object consisting of series of object names, e.g. [database,schema,table,index]
        :return: ReflectedEntity of object
        """
        # Get child object by name
        child_obj = self.get_child_by_name(object_names[0])

        # If there are remaining object IDs to resolve, recurse further
        if len(object_names) > 1:
            return child_obj.get_object_by_id(*object_names[1:])
        else:
            return child_obj

    def get_child_by_name(self, name):
        """Try and find child of any type with specified name"""
        for child_class in self.child_objects:
            try:
                return self.get_child(child_class.object_type, name)
            except exceptions.DBObjectDoesntExistError:
                continue
        else:
            raise exceptions.DBObjectDoesntExistError('{} does not have any child named: "{}"'.format(self, name))

    def __eq__(self, other):
        if isinstance(other, reflected.ReflectedEntity):
            # Use ReflectedEntity __eq__ logic
            return other == self
        else:
            return super().__eq__(other)

    def __str__(self):
        return '{}: {}'.format(type(self).__name__, self.name)

    def display_details(self):
        return '{} with attributes: {}'.format(self, {attr: getattr(self,attr) for attr in self.valid_attributes})


class Column(DeclaredEntity):
    """
    Base class for Columns
    """
    object_type = 'columns'

    def __init__(self, name, data_type, identity=False, nullable=False, char_max_len=None, datetime_precision=None, numeric_precision=None, numeric_scale=None, old_name=None):
        super().__init__(name, old_name=old_name, data_type=data_type, identity=identity, nullable=nullable, char_max_len=char_max_len,
                         datetime_precision=datetime_precision, numeric_precision=numeric_precision,
                         numeric_scale=numeric_scale)

    def sql_representation(self):
        return column_sql_representation(self.name, self.data_type, identity=self.identity, nullable=self.nullable,
                                         char_max_len=self.char_max_len,
                                         datetime_precision=self.datetime_precision,
                                         numeric_precision=self.numeric_precision,
                                         numeric_scale=self.numeric_scale)

    def data_type_representation(self):
        return data_type_representation(self.data_type,
                                        char_max_len=self.char_max_len,
                                        datetime_precision=self.datetime_precision,
                                        numeric_precision=self.numeric_precision,
                                        numeric_scale=self.numeric_scale)


class IntegerColumn(Column):
    def __init__(self, name, **kwargs):
        super().__init__(name, 'int',  **kwargs)


class FloatColumn(Column):
    def __init__(self, name, small=False, **kwargs):
        """

        :param name:
        :param bool small: Small float takes 4 bytes, large takes 8
        :param kwargs:
        """
        super().__init__(name, 'float', numeric_precision=24 if small else 53, **kwargs)


class VarcharColumn(Column):
    def __init__(self, name, char_max_len, **kwargs):
        super().__init__(name, 'varchar', char_max_len=char_max_len, **kwargs)


class DateColumn(Column):
    def __init__(self, name,  **kwargs):
        super().__init__(name, 'date', **kwargs)


class DateTimeColumn(Column):
    def __init__(self, name,  datetime_precision=7, **kwargs):
        super().__init__(name, 'datetime2', datetime_precision=datetime_precision, **kwargs)


class IdentityColumn(IntegerColumn):

    def __init__(self, name, **kwargs):
        super().__init__(name, identity=True, nullable=False, **kwargs)


class NumericColumn(Column):

    def __init__(self, name, numeric_precision, numeric_scale, **kwargs):
        if numeric_scale >= numeric_precision:
            raise ValueError('{} Numeric Scale must be less than Numeric Precision'.format(type(self).__name__))
        super().__init__(name, 'numeric', numeric_precision=numeric_precision, numeric_scale=numeric_scale, **kwargs)


class ForeignKey(DeclaredEntity):
    """
    Class for defining foreign key
    Name of FK is automatically created upon generation
    """
    object_type = 'foreign_keys'

    def __init__(self, column, foreign_table, foreign_column, foreign_schema='dbo'):
        """

        :param str column: Name of FROM column of FK
        :param str foreign_schema: Foreign schema name
        :param str foreign_table: Foreign table name
        :param str foreign_column: Foreign column name
        """
        super().__init__('', column=column, foreign_schema=foreign_schema, foreign_table=foreign_table,
                         foreign_column=foreign_column)

    def __str__(self):
        return "{} from: '{}' to: '{}.{}.{}'".format(type(self).__name__, self.column,
                                                     self.foreign_schema, self.foreign_table, self.foreign_column)


class AbstractIndex(DeclaredEntity):
    """
    Base class for Primary Keys and Indexes
    """
    name_prefix = ''

    def __init__(self, columns, name=None, compression='NONE', **kwargs):
        """
        Name will be automatically generated from columns if not provided
        :param list/tuple/str columns: List of names of columns index applies to, or single column name
        :param bool clustered: Whether PK/index is clustered
        :param str compression: Compression Type (NONE, ROW or PAGE)
        :param kwargs:
        """
        if isinstance(columns, str):
            columns = [columns]
        columns = tuple(column_name.lower() for column_name in columns)

        # Automatically create name from columns
        if not name:
            name = '_'.join((self.name_prefix,) + columns)
            included_columns = kwargs.get('included_columns', None)
            if included_columns:
                name += '__' + '_'.join(included_columns)

        super().__init__(name,
                         columns=columns,
                         compression=compression,
                         **kwargs)


class PrimaryKey(AbstractIndex):
    object_type = 'primary_keys'
    name_prefix = 'PK'

    def __init__(self, columns, clustered=True, **kwargs):
        super().__init__(columns, clustered=clustered, **kwargs)

    def __str__(self):
        return '{} on columns: {}'.format(type(self).__name__, self.columns)


class Index(AbstractIndex):
    object_type = 'indexes'
    name_prefix = 'IX'

    def __init__(self, columns, clustered=False, unique=False, included_columns=None, **kwargs):
        """

        :param str/list columns: single column name or sequence of column names
        :param clustered:
        :param included_columns:
        :param unique:
        :param kwargs:
        """

        super().__init__(columns=columns,
                         clustered=clustered,
                         included_columns=set(column_name.lower() for column_name in included_columns)
                         if included_columns else None,
                         unique=unique,
                         **kwargs)

    def __str__(self):
        rep = '{} on columns: {}'.format(type(self).__name__, self.columns)
        if self.included_columns:
            rep += ' with included columns: {}'.format(self.included_columns)
        return rep


class Partition(DeclaredEntity):
    """
    Class for defining partition on a datetime column, with daily partitions
    Handles creation of partition function and partition scheme
    Must specify column to create partition on
    Do not need to specify name, generated automatically upon creation

    """
    object_type = 'partitions'

    def __init__(self, column):
        super().__init__('', column=column.lower())

    def __str__(self):
        return '{} on column: {}'.format(type(self).__name__, self.column)


class Table(DeclaredEntity):
    object_type = 'tables'
    child_types = (Column, PrimaryKey, Index, Partition)

    def __init__(self, name, primary_key=None, partition=None, **children):
        # Only one primary key
        super().__init__(name,
                         primary_keys=[primary_key] if primary_key else None,
                         partitions=[partition] if partition else None, **children)

    def get_clustered_index_fields(self):
        try:
            return next(index.columns
                        for index in self.get_children('indexes')
                        if index.clustered)
        except StopIteration:
            return None


class Schema(DeclaredEntity):
    object_type = 'schemas'
    child_types = (Table,)


class User(DeclaredEntity):
    object_type = 'users'

    def __init__(self, name, login_name, db_roles=None):
        super().__init__(name, login_name=login_name, db_roles=set(db_roles or set()))

    @classmethod
    def for_login(cls, login, **kwargs):
        return cls(login.name, login.name, **kwargs)

    def __str__(self):
        if self.name:
            return '{}: {}'.format(type(self).__name__, self.name)
        else:
            return '{} for login: {}'.format(type(self).__name__, self.login_name)


class Login(DeclaredEntity):
    object_type = 'logins'

    def __init__(self, name, password=None, type_desc='SQL_LOGIN', server_roles=None):
        super().__init__(name, password=password, type_desc=type_desc, server_roles=set(server_roles or set()))


class Database(DeclaredEntity):
    object_type = 'databases'
    child_types = (Schema, User)

    def __init__(self, name, owner, data_file_dir=None, log_file_dir=None, data_size=None, log_size=None, recovery_model_desc='FULL',
                 data_file_name=None, log_file_name=None, tables=None, **kwargs):
        if recovery_model_desc.lower() not in {'full', 'simple', 'bulk_logged'}:
            raise ValueError('Invalid database recovery level: {}'.format(recovery_model_desc))

        if data_file_dir is not None:
            # Handle specifying data and log file details
            if not data_size:
                raise ValueError('Data size must be specified if data file name is provided')

            log_size = log_size or data_size/10
            data_file_name = data_file_name or '{}.mdf'.format(name)
            log_file_name = log_file_name or '{}_log.ldf'.format(name)
            data_file_path = os.path.join(data_file_dir, data_file_name).replace('/', '\\')
            log_file_path = os.path.join(log_file_dir, log_file_name).replace('/', '\\')
        else:
            # Use defaults
            data_file_path = None
            log_file_path = None

        # Allow specifying tables directly as a shorthand
        if tables:
            if "schemas" in kwargs:
                raise ValueError('Cannot specify both "tables" and "schemas" in database declaration')
            # Use default "dbo" schema
            kwargs["schemas"] = [Schema('dbo', tables=tables)]

        super().__init__(name, owner=owner,
                         # TODO: Make more flexible attribute equality to allow comparing file paths with different seperators
                         data_file_path=data_file_path,
                         log_file_path=log_file_path,
                         data_size=data_size,
                         log_size=log_size,
                         recovery_model_desc=recovery_model_desc, **kwargs)

    def get_table(self, table_name, schema_name='dbo'):
        """
        Shortcut for getting table
        :param table_name:
        :param schema_name:
        :return:
        """
        schema = self.get_child('schemas', schema_name)
        return schema.get_child('tables', table_name)

    def add_table(self, table, schema_name='dbo'):
        """
        Shortcut for adding table to database
        :param table:
        :param schema_name:
        :return:
        """
        try:
            schema = self.get_child('schemas', schema_name)
        except exceptions.InvalidDBEntityChildError:
            schema = Schema(schema_name)
            self.add_child(schema)

        schema.add_child(table)


class Server(DeclaredEntity):
    object_type = 'servers'
    child_types = (Login, Database)

    def __init__(self, **kwargs):
        """
        Server has no name
        """
        super().__init__('', **kwargs)
