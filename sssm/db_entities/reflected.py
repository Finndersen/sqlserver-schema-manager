import logging
from datetime import date, timedelta

from sssm import sql, exceptions, util
from sssm.db_entities import attributes, declared

log = logging.getLogger(__name__)


class ReflectedEntity(object):
    """
    Base SQLServer object which has associated server connection to perform actions
    """
    # Tuple of child object types of this DB object (Reflected Entity subclasses)
    child_types = tuple()
    # Identitifer for type of DB object (e.g. 'databases', 'tables')
    object_type = None
    # List of system-defined object names (to ignore)
    system_names = []
    valid_attributes = {}
    can_create = True

    def __init__(self, parent, name, cur=None):
        if name is None:
            raise exceptions.DBError('Reflected DB entity must be defined with a name')
        self.name = name
        self.parent = parent
        if cur:
            self.cur = cur
        else:
            self.cur = parent.cur

        self.extra_init()

        self.valid_attributes = attributes.valid_attributes[self.object_type]
        # Dictionary used to store attribute values
        self._attributes = {}
        # used to store row result of object attribute detail query
        self._detail_result = None

    def __getattr__(self, item):
        """
        Catch attribute lookup for DB entitiy attributes. Allows lazy evaluation
        :param str item:
        :return:
        """
        if item not in self.valid_attributes:
            raise AttributeError('"{}" is not a valid attribute of {}'.format(item, type(self).__name__))

        if self._detail_result is None:
            self._detail_result = self.get_details()

        # Get attribute value
        if item not in self._attributes:
            self._attributes[item] = self.read_attribute(self._detail_result, item)

        return self._attributes[item]

    def __setattr__(self, key, value):
        """Restrict setting object attributes"""
        if key in self.valid_attributes:
            raise AttributeError('Do not directly set attribute: "{}"'.format(key))
        super().__setattr__(key, value)

    def ex(self, sql_str, commit=False, **kwargs):
        """
        Convenience method for executing SQL statement
        :param str sql_str: SQL to execute
        :param bool commit: Whether to commit afterwards
        :param kwargs:
        :return:
        """
        cur = self.cur.execute(sql_str, **kwargs)
        if commit:
            cur.commit()
        return cur

    def extra_init(self):
        pass

    ###########################################################################################
    #       INITIALISATION METHODS
    ###########################################################################################
    @classmethod
    def all_for_parent(cls, parent):
        """Return all objects belonging to parent"""
        return [cls(parent, name) for name in cls._list_names_ex(parent) if name not in cls.system_names]

    @classmethod
    def from_declared(cls, parent, declared_obj, **kwargs):
        """
        Get object by corresponding to declared version
        Often just redirects to 'by_name' method, some special cases might want to change behaviour (e.g. indexes)
        """
        return cls.by_name(parent, declared_obj.name, **kwargs)

    @classmethod
    def by_name(cls, parent, name, **kwargs):
        """
        Get object by name (unique for parent)
        This will be used by from_declared in most cases
        """
        if name and cls._name_exists_ex(parent, name):
            return cls(parent, name, **kwargs)
        else:
            raise exceptions.DBObjectDoesntExistError('{}: {} does not exist'.format(cls.__name__, name))

    @classmethod
    def get_or_create(cls, parent, declared_object):
        """Get existing reflected object (if name matches) or create new one"""
        # Check for existing object
        try:
            return cls.from_declared(parent, declared_object)
        except exceptions.DBObjectDoesntExistError as e:
            if cls.can_create:
                # Create object
                cls.create(parent, declared_object)
                # Get child details from database to verify creation
                return cls.from_declared(parent, declared_object)
            else:
                log.info('Unable to create: {}'.format(declared_object))
                return None

    ###########################################################################################
    #       HIGH LEVEL CHILD GENERATION METHODS
    ###########################################################################################
    def rename_child_with_old_name(self, declared_obj):
        """

        :param declared_obj:
        :return:
        """
        try:
            old_name_child = self.get_child(declared_obj.object_type, declared_obj.old_name)
            old_name_child.rename(declared_obj.name)
        except exceptions.DBObjectDoesntExistError as e:
            pass

    def get_or_create_child(self, declared_object):
        """Get or create child object of corresponding type"""
        # Get class of child object
        child_class = self.get_child_class_from_name(declared_object.object_type)
        return child_class.get_or_create(self, declared_object)

    def get_children(self, child_type):
        """Get all children objects of particular type"""
        child_class = self.get_child_class_from_name(child_type)
        return child_class.all_for_parent(self)

    def get_child_from_declared(self, declared_child):
        """Get particular instance of child type by delcared object"""
        child_class = self.get_child_class_from_name(declared_child.object_type)
        return child_class.from_declared(self, declared_child)

    def get_child(self, child_type, name, **kwargs):
        """Get particular instance of child type by name"""
        child_class = self.get_child_class_from_name(child_type)
        return child_class.by_name(self, name, **kwargs)

    def get_child_by_name(self, name):
        """Try and find child of any type with specified name"""
        for child_class in self.child_types:
            try:
                return self.get_child(child_class.object_type, name)
            except exceptions.DBObjectDoesntExistError:
                continue
        else:
            raise exceptions.DBObjectDoesntExistError('{} does not have any child named: "{}"'.format(self, name))

    def get_child_class_from_name(self, child_type):
        for child_class in self.child_types:
            if child_class.object_type == child_type:
                return child_class
        else:
            raise exceptions.InvalidDBEntityChildError(
                'DB Entity: "{}" does not have child type: "{}"'.format(type(self).__name__, child_type))

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

    ###########################################################################################
    #       HIGH LEVEL MANIPULATION METHODS
    ###########################################################################################
    def read_attribute(self, detail_result, attribute_name):
        """
        Entry point method for getting and translating attribute value
        Redirects to 'get_attr_<attribute_name>' method which will use SQL to retrieve attribute from DB
        :param pyodbc.Row detail_result:details of object retrieved from DB
        :param str attribute_name: name of attribute to read value for
        :return:
        """
        method = getattr(self, 'get_attr_{}'.format(attribute_name), None)
        if method:
            return method(detail_result)
        else:
            # If method doesnt exist, just take attribute name directly from detail result if present
            try:
                return getattr(detail_result, attribute_name)
            except AttributeError:
                raise AttributeError(
                    'No method defined for getting attribute: "{}", and attribute name not in detail query result'.format(
                        attribute_name))

    def reset_attribute(self, attribute_name):
        """
        Reset attribute so it will be refreshed on next lookup
        :param attribute_name:
        :return:
        """
        self._detail_result = None
        self._attributes.pop(attribute_name, None)

    def reset_all_attributes(self):
        """
        Reset all attributes so they will be refreshed on next lookup
        :return:
        """
        self._detail_result = None
        self._attributes = {}

    def set_attribute(self, declared_obj, attribute_name):
        """
        Entry point method for altering attributes.
        Will redirect to 'set_<attribute_name> method which will use SQL to alter attribute
        Returns boolean indicating whether attribute was set successfully
        """
        # Alter attribute
        method = getattr(self, 'set_attr_{}'.format(attribute_name), None)
        if not method:
            log.error('No method defined for altering {} attribute: "{}"'.format(self.full_name(), attribute_name))
            return False
            # raise exceptions.DefinitionError('No method defined for altering attribute: "{}"'.format(attribute_name))

        new_value = getattr(declared_obj, attribute_name)
        if input('Are you sure you want to set: {} "{}" to {}? (y/n)'.format(self.full_name(), attribute_name,
                                                                             new_value)) == 'y':
            log.info('Setting {} "{}" = {}'.format(self, attribute_name, new_value))
            method(declared_obj)
            self.cur.commit()
            # Validate attribute change
            self.reset_attribute(attribute_name)
            if getattr(self, attribute_name) != new_value:
                raise exceptions.DBNotAlteredAttributeError(
                    '{} attribute: "{}" was not updated to: "{}"'.format(type(self).__name__, attribute_name,
                                                                         new_value))
            return True
        else:
            return False

    ###########################################################################################
    #       CRUD ETC METHODS TO INTERACT WITH DB WITH SQL
    ###########################################################################################
    @classmethod
    def create(cls, parent, declared_object):
        # Perform create
        log.info('Creating: {}: {}'.format(parent.full_name(), declared_object))
        cls._create_ex(parent, declared_object)
        parent.cur.commit()
        # Verify
        if not cls._name_exists_ex(parent, declared_object.name):
            raise exceptions.DBError('Problem creating DB entity for: {}'.format(declared_object))

    @classmethod
    def _create_ex(cls, parent, declared_object):
        """
        Execute SQL to create database object matching delcared object attributes
        """
        raise NotImplementedError()

    @classmethod
    def _name_exists_ex(cls, parent, name):
        """
        SQL to check whether object with name exists in DB
        :param parent:
        :param name:
        :return:
        Example:
        return parent.cur.ex(cls.exists_sql.format(**extra_params, **{cls.exists_name_field: name})).fetchone()
        """
        raise NotImplementedError()

    @classmethod
    def _list_names_ex(cls, parent):
        """
        Method used to execute SQL to get list of object names belonging to parent
        example:
        return [getattr(result, cls.list_name_field) for result in parent.cur.ex(cls.list_sql.format(**extra_params)).fetchall()]
        """
        raise NotImplementedError()

    def rename(self, new_name):
        """
        Handle renaming and verification
        :param new_name:
        :return:
        """
        if input('Are you sure you want to rename {} to "{}"? (y/n)'.format(self.full_name(), new_name)) == 'y':
            log.info('Renaming {} to {}'.format(self.full_name(), new_name))
            self._rename_ex(new_name)
            self.cur.commit()
            # Verify name change
            if not self._name_exists_ex(self.parent, new_name):
                raise exceptions.DBError('Problem renaming {} to {}'.format(self.full_name(), new_name))
            self.name = new_name

    def _rename_ex(self, new_name):
        """
        Perform rename action
        :param new_name:
        :return:
        """
        raise NotImplementedError()

    def delete(self):
        # Check whether object is allowed to be deleted
        if self.can_delete():
            if input('Are you sure you want to delete {}: "{}"? (y/n)'.format(type(self).__name__,
                                                                              self.full_name())) == 'y':
                log.info('Deleting {}: {}'.format(type(self).__name__, self.full_name()))
                self._delete_ex()
                self.cur.commit()
            else:
                return False
        else:
            # Log can't delete
            log.info('Delete not allowed for: {}'.format(self))
            return False

    def can_delete(self):
        """Define condition for whether this object should be removed automatically if not declared"""
        return True

    def _delete_ex(self):
        """
        Perform SQL statement to delete object
        :return: 
        """
        raise NotImplementedError()

    def get_details(self):
        details = self._get_details_ex()
        if not details:
            raise exceptions.DBError('Problem getting details for: {}'.format(self))
        return details

    def _get_details_ex(self):
        """
        Method used to execute SQL to get details of current object
        return self.cur.ex(self.detail_sql.format(**extra_params, **{self.detail_name_field: self.name})).fetchone()
        """
        raise NotImplementedError()

    ###########################################################################################
    #       MISC METHODS
    ###########################################################################################
    def full_name(self, max_ancestor='databases'):
        """Get full name of entity up to specified max level"""
        name_list = []
        current_obj = self

        while current_obj.object_type != max_ancestor:
            name_list.append(current_obj.name)
            current_obj = current_obj.parent
            if current_obj is None:
                break
        if current_obj:
            name_list.append(current_obj.name)
        return '.'.join(name_list[::-1])

    def ancestor_name(self, ancestor_type):
        """
        Get name of ancestor for given ancestor type
        :param ancestor_type:
        :return:
        """
        ancestor = self
        while ancestor.object_type != ancestor_type:
            ancestor = ancestor.parent
            if ancestor is None:
                return None
        return ancestor.name

    def __eq__(self, other):
        if isinstance(other, declared.DeclaredEntity):
            return self.object_type and (self.object_type == other.object_type) and self.equate_declared(other)
        else:
            return super().__eq__(other)

    def equate_declared(self, declared_obj):
        """
        Used to determine whether this Reflected object is 'the same' as a declared object
        For named objects, compare names or name and old name
        :param declared_obj:
        :return:
        """
        return self.name.lower() == declared_obj.name.lower() or (
                declared_obj.old_name and self.name.lower() == declared_obj.old_name.lower())

    def __str__(self):
        return '{}:{}'.format(type(self).__name__, self.full_name())

    def display_details(self):
        return '{} with attributes: {}'.format(self, {attr: getattr(self, attr) for attr in self.valid_attributes})


###########################################################################################
#       PRIMARY KEY
###########################################################################################
class ReflectedPrimaryKey(ReflectedEntity):
    object_type = 'primary_keys'
    name_prefix = 'pk'

    @classmethod
    def _list_names_ex(cls, parent):
        result = parent.ex(sql.TABLE_PK_NAME.format(schema=parent.parent.name, table=parent.name)).fetchone()
        if result:
            return [result.name]
        else:
            return []

    @classmethod
    def _name_exists_ex(cls, parent, name):
        return bool(
            parent.ex(sql.PK_EXISTS.format(index_name=name, schema=parent.parent.name, table=parent.name)).fetchone())

    @classmethod
    def _create_ex(cls, parent, declared_object):
        # TODO: Delete existing clustered index if this index is clustered
        cls.create_helper(parent, declared_object, drop_existing=False)

    def _get_details_ex(self):
        return self.ex(sql.named_index_details.format(schema=self.parent.parent.name,
                                                      table=self.parent.name,
                                                      index_name=self.name)).fetchone()

    def _delete_ex(self):
        self.ex(sql.DROP_CONSTRAINT.format(schema=self.parent.parent.name,
                                           table=self.parent.name,
                                           constraint_name=self.name))

    def _rename_ex(self, new_name):
        self.ex(sql.RENAME_INDEX.format(schema=self.parent.parent.name,
                                        table=self.parent.name,
                                        old_name=self.name,
                                        new_name=new_name), commit=True)
        self.name = new_name

    @classmethod
    def from_declared(cls, parent, declared_obj, **kwargs):
        # Get name of PK with columns corresponding to declared object
        # Get details of PK on table
        pk_details = parent.ex(
            sql.TABLE_PK_COLUMNS.format(schema_name=parent.parent.name, table_name=parent.name)).fetchall()
        # Check if PK columns match declared
        if [pk.column_name.lower() for pk in pk_details] == [column_name.lower() for column_name in
                                                             declared_obj.columns]:
            return cls(parent, name=pk_details[0].pk_name, **kwargs)
        else:
            raise exceptions.DBObjectDoesntExistError(
                'Could not find any existing primary key matching definition of: {}'.format(declared_obj))

    @classmethod
    def create_helper(cls, parent, from_obj, name=None, drop_existing=False, **kwargs):
        """
        Helper function used for creating or re-creating PK
        :param parent: Parent relfected object
        :param from_obj: Reflected or declared object to use attributes from
        :param name: Index name
        :param drop_existing: whether to drop existing PK of same name
        :return:
        """
        name = name or from_obj.name
        # Determine filegroup to create on (TBA - need to check parent table for partitioning)
        parent.ex(sql.CREATE_PK.format(schema=parent.parent.name,
                                       table=parent.name,
                                       pk_name=name,
                                       columns=', '.join(from_obj.columns),
                                       clustering='CLUSTERED' if from_obj.clustered else 'NONCLUSTERED',
                                       compression=from_obj.compression))

    def get_attr_columns(self, detail):
        # If table is partitioned then the partition column will be included in the list of index columns
        # First check for all columns not involved in partition (case for non-partitioned index)
        non_partition_columns = tuple(result.column_name.lower()
                                      for result in
                                      self.ex(sql.index_nonpartition_columns.format(schema=self.parent.parent.name,
                                                                                    table=self.parent.name,
                                                                                    index_name=self.name)).fetchall())
        if non_partition_columns:
            return non_partition_columns
        else:
            # If none found, try for all columns (for case of Partition Index)
            index_column_names = tuple(result.column_name.lower()
                                       for result in
                                       self.ex(sql.index_all_columns.format(schema=self.parent.parent.name,
                                                                            table=self.parent.name,
                                                                            index_name=self.name)).fetchall())
            if not index_column_names:
                raise Exception('Problem getting columns of index: {} of {}'.format(self.name, self.parent))
            return index_column_names

    def get_attr_clustered(self, detail_result):
        return detail_result.type_desc == 'CLUSTERED'

    def set_attr_compression(self, declared_obj, online=False):
        """

        :param declared_obj:
        :param bool online: Whether to rebuild index in Online mode
        :return:
        """
        self.ex(sql.ALTER_INDEX.format(schema=self.parent.parent.name,
                                       table=self.parent.name,
                                       index_name=self.name,
                                       compression=declared_obj.compression,
                                       online='ON' if online else 'OFF'), commit=True)

    def set_attr_clustered(self, declared_obj):
        self.recreate_new_attributes(declared_obj)

    def recreate_new_attributes(self, declared_obj):
        """
        Helper function to re-create index with new declared attributes
        :param declared_obj: Declared Index to recreate index with.
        :return:
        """
        self.create_helper(self.parent, declared_obj, self.name, drop_existing=True)

    def recreate_new_filegroup(self, create_on):
        """
        Helper function for recreating index on new filegroup (to add or remove partitioning)
        :param create_on: name of partition scheme and column, or PRIMARY filegroup
        :return:
        """
        log.info('Recreating index: {} on Filegroup: {}'.format(self, create_on))
        self.create_helper(self.parent, self, drop_existing=True, create_on=create_on)

    def equate_declared(self, declared_obj):
        """
        Equate based on columns in index/PK
        :param declared_obj:
        :return:
        """
        log.debug('Comparing {} to {}'.format(self, declared_obj))
        return self.columns == declared_obj.columns

    def includes_column(self, column_name):
        """
        Check if this index includes specified column
        :param str column_name:
        :return:
        """
        return column_name in set(self.columns)

    def __str__(self):
        return super().__str__() + ' on columns: {}'.format(self.columns)


###########################################################################################
#       INDEX
###########################################################################################
class ReflectedIndex(ReflectedPrimaryKey):
    object_type = 'indexes'
    name_prefix = 'ix'

    @classmethod
    def _list_names_ex(cls, parent):
        return [result.name for result in
                parent.ex(sql.TABLE_INDEX_NAMES.format(schema=parent.parent.name, table=parent.name)).fetchall()]

    @classmethod
    def _name_exists_ex(cls, parent, name):
        return bool(parent.ex(
            sql.INDEX_EXISTS.format(index_name=name, schema=parent.parent.name, table=parent.name)).fetchone())

    @classmethod
    def from_declared(cls, parent, declared_obj, **kwargs):
        # GO through all indexes on table and see if any match with declared columns
        for index in cls.all_for_parent(parent):
            # If columns match, return instance with name
            if index == declared_obj:
                log.debug('Declared Index: {} matches existing index: "{}"'.format(declared_obj, index))
                return index
        raise exceptions.DBObjectDoesntExistError(
            'Could not find any existing index matching definition of: {}'.format(declared_obj))

    @classmethod
    def create_helper(cls, parent, from_obj, name=None, drop_existing=False, create_on=None):
        """
        Helper function used for creating or re-creating index
        :param parent: Parent relfected object
        :param from_obj: Reflected or declared index object containing attributes to make index from
        :param name: Index name (will use that of from_obj if not supplied)
        :param drop_existing: whether to drop existing index of same name
        :return:
        """
        # Get name
        name = name or from_obj.name
        # Get included columns
        if from_obj.included_columns:
            include_str = "INCLUDE ({})".format(', '.join(from_obj.included_columns))
        else:
            include_str = ''
        # If CREATE ON is not specified, determine automatically by checking parent table for partition scheme
        # (will only work on non-clustered indexes)
        if create_on is None:
            table_partitions = parent.get_children('partitions')
            if table_partitions:
                partition = table_partitions[0]
                create_on = '{}({})'.format(partition.name, partition.column)
            else:
                create_on = '[PRIMARY]'

        parent.ex(sql.CREATE_INDEX.format(schema=parent.parent.name,
                                          table=parent.name,
                                          name=name,
                                          columns=', '.join(from_obj.columns),
                                          unique='UNIQUE' if from_obj.unique else '',
                                          clustering='CLUSTERED' if from_obj.clustered else 'NONCLUSTERED',
                                          drop_existing='ON' if drop_existing else 'OFF',
                                          include=include_str,
                                          compression=from_obj.compression,
                                          create_on=create_on))

    def _delete_ex(self):
        # Check if index belongs to unique constraint
        if bool(self._get_details_ex().is_unique_constraint):
            # Drop constraint
            log.info('Dropping unique constraint of index...')
            self.ex(sql.DROP_CONSTRAINT.format(schema=self.parent.parent.name,
                                               table=self.parent.name,
                                               constraint_name=self.name))
        else:
            self.ex(sql.DROP_INDEX.format(index_name=self.name, schema=self.parent.parent.name, table=self.parent.name))

    def get_attr_included_columns(self, detail):
        included_columns = set(result.column_name.lower() for result in
                               self.ex(sql.index_included_columns.format(schema=self.parent.parent.name,
                                                                         table=self.parent.name,
                                                                         index_name=self.name)).fetchall())
        return included_columns or None

    def set_attr_included_columns(self, declared_obj):
        self.recreate_new_attributes(declared_obj)

    def equate_declared(self, declared_obj):
        """
        Also compare included columns
        :param declared_obj:
        :return:
        """
        return super().equate_declared(declared_obj) and self.included_columns == declared_obj.included_columns

    def __str__(self):
        rep = super().__str__()
        if self.included_columns:
            rep += ' with included columns: {}'.format(self.included_columns)
        return rep


###########################################################################################
#       FOREIGN KEY
###########################################################################################
class ReflectedForeignKey(ReflectedEntity):
    object_type = 'foreign_keys'

    @classmethod
    def _list_names_ex(cls, parent):
        return [result.name for result in
                parent.ex(sql.TABLE_FOREIGN_KEYS.format(schema=parent.parent.name, table=parent.name)).fetchall()]

    @classmethod
    def _create_ex(cls, parent, declared_object):
        fk_name = '_'.join(['FK', parent.parent.name, parent.name, declared_object.foreign_schema,
                            declared_object.foreign_table])
        parent.ex(sql.CREATE_FOREIGN_KEY.format(schema=parent.parent.name,
                                                table=parent.name,
                                                fk_name=fk_name,
                                                column=declared_object.column,
                                                foreign_schema=declared_object.foreign_schema,
                                                foreign_table=declared_object.foreign_table,
                                                foreign_column=declared_object.foreign_column))

    @classmethod
    def from_declared(cls, parent, declared_obj, **kwargs):
        # GO through all FKs on table and see if any match with declared FK configuration
        for fk_name in cls._list_names_ex(parent):
            fk_detail = parent.ex(sql.FK_DETAILS.format(schema=parent.parent.name,
                                                        table=parent.name,
                                                        fk_name=fk_name)).fetchone()
            # If columns match, return instance with name
            if all((getattr(fk_detail, attr) == getattr(declared_obj, attr) for attr in
                    ('column', 'foreign_schema', 'foreign_table', 'foreign_column'))):
                return cls(parent, name=fk_name)
        raise exceptions.DBObjectDoesntExistError(
            'Could not find any existing foreign key matching definition of: {}'.format(declared_obj))

    def _delete_ex(self):
        self.ex(sql.DROP_CONSTRAINT.format(schema=self.parent.parent.name,
                                           table=self.parent.name,
                                           constraint_name=self.name))

    def _get_details_ex(self):
        return self.ex(sql.FK_DETAILS.format(schema=self.parent.parent.name,
                                             table=self.parent.name,
                                             fk_name=self.name)).fetchone()


###########################################################################################
#       COLUMN
###########################################################################################
class ReflectedColumn(ReflectedEntity):
    object_type = 'columns'

    @classmethod
    def _list_names_ex(cls, parent):
        return [result.name.lower() for result in
                parent.ex(sql.table_column_details.format(schema=parent.parent.name, table=parent.name)).fetchall()]

    @classmethod
    def _name_exists_ex(cls, parent, name):
        return bool(parent.ex(
            sql.COLUMN_EXISTS.format(schema=parent.parent.name, table=parent.name, column_name=name)).fetchone())

    @classmethod
    def _create_ex(cls, parent, declared_object):
        column_sql = declared_object.sql_representation()
        parent.ex(sql.ADD_COLUMN.format(schema=parent.parent.name, table=parent.name, column_def=column_sql))

    def _get_details_ex(self):
        return self.ex(sql.column_detail.format(schema=self.parent.parent.name,
                                                table=self.parent.name,
                                                column_name=self.name)).fetchone()

    def _rename_ex(self, new_name):
        self.ex(sql.RENAME_COLUMN.format(schema=self.parent.parent.name,
                                         table=self.parent.name,
                                         old_name=self.name,
                                         new_name=new_name))

    def get_attr_nullable(self, detail):
        return bool(detail.nullable)

    def get_attr_identity(self, detail):
        return bool(detail.identity)

    def get_attr_numeric_precision(self, detail):
        # Numeric precision only relevant for some numeric types
        if detail.data_type in util.NUMERIC_TYPES | util.APPROX_NUMBER_TYPES:
            return detail.numeric_precision
        else:
            return None

    def get_attr_numeric_scale(self, detail):
        # Numeric scale only relevant for some numeric types
        if detail.data_type in util.NUMERIC_TYPES:
            return detail.numeric_scale
        else:
            return None

    def get_attr_datetime_precision(self, detail):
        if detail.data_type in util.DATETIME_PRECISION_TYPES:
            return detail.datetime_precision
        else:
            return None

    def get_attr_char_max_len(self, detail):
        if detail.data_type in util.CHAR_TYPES:
            return detail.char_max_len
        else:
            return None

    def set_attr_identity(self, declared_obj):
        # Check if table already has PK
        if self.parent.get_pk_fields():
            raise Exception('Cannot make {} an identity column because {} already has a primary key'.format(self,
                                                                                                            self.parent))
        # Must delete and re-create to change identity attribute
        self.delete()
        self.create(self.parent, declared_obj)

    def set_attr_nullable(self, declared_obj):
        self._alter_column(declared_obj)

    def set_attr_char_max_len(self, declared_obj):
        self._alter_column(declared_obj)

    def set_attr_datetime_precision(self, declared_obj):
        self._alter_column(declared_obj)

    def set_attr_numeric_precision(self, declared_obj):
        self._alter_column(declared_obj)

    def set_attr_numeric_scale(self, declared_obj):
        self._alter_column(declared_obj)

    def set_attr_data_type(self, declared_obj):
        # Drop any indexes that column is included in
        if self.drop_associated_indexes():
            self._alter_column(declared_obj)
        else:
            # Dont delete column if any indexes were not removed
            log.info('Column: {} will not be altered'.format(self.name))

    def _alter_column(self, declared_obj):
        """
        Alter Column to match declared objects attributes. Used to set just about all column attributes
        :param declared_obj:
        :return:
        """
        self.ex(sql.ALTER_COLUMN.format(schema=self.parent.parent.name, table=self.parent.name,
                                        column_def=declared_obj.sql_representation()),
                commit=True)
        # Reset attributes because they may have been changed
        self.reset_all_attributes()

    def drop_associated_indexes(self):
        column_indexes = self.parent.get_indexes_for_column(self.name)
        for index in column_indexes:
            if not index.delete():
                return False
        return True

    def _delete_ex(self):
        if self.drop_associated_indexes():
            # Drop column
            self.ex(sql.DROP_COLUMN.format(schema=self.parent.parent.name,
                                           table=self.parent.name,
                                           column_name=self.name),
                    commit=True)
        else:
            # Dont delete column if any indexes were not removed
            log.info('Column: {} will not be deleted'.format(self.name))

    def sql_representation(self):
        return util.column_sql_representation(self.name, self.data_type, identity=self.identity, nullable=self.nullable,
                                              char_max_len=self.char_max_len,
                                              datetime_precision=self.datetime_precision,
                                              numeric_precision=self.numeric_precision,
                                              numeric_scale=self.numeric_scale)

    def data_type_representation(self):
        return util.data_type_representation(self.data_type,
                                             char_max_len=self.char_max_len,
                                             datetime_precision=self.datetime_precision,
                                             numeric_precision=self.numeric_precision,
                                             numeric_scale=self.numeric_scale)


###########################################################################################
#       TABLE PARTITION
###########################################################################################
class ReflectedPartition(ReflectedEntity):
    """
    Represents a partition on a table. Manages corresponding partition function and scheme
    """

    object_type = 'partitions'

    @classmethod
    def _list_names_ex(cls, parent):
        # Get names of partition schemes applied to parent table
        return [result.ps_name for result in parent.ex(sql.table_partition_names.format(schema=parent.parent.name,
                                                                                        table=parent.name)).fetchall()]

    @classmethod
    def _name_exists_ex(cls, parent, name):
        return bool(parent.ex(sql.partition_name_exists_on_table.format(schema=parent.parent.name,
                                                                        table=parent.name,
                                                                        ps_name=name)).fetchone())

    @classmethod
    def from_declared(cls, parent, declared_obj, **kwargs):
        # Get existing partition scheme name for table and column
        partition_detail = parent.ex(sql.table_partition_details_for_column.format(schema=parent.parent.name,
                                                                                   table=parent.name,
                                                                                   column_name=declared_obj.column)).fetchone()
        if partition_detail:
            return cls(parent, name=partition_detail.ps_name)
        else:
            raise exceptions.DBObjectDoesntExistError(
                'Could not find any existing partition matching definition of: {}'.format(declared_obj))

    @classmethod
    def _create_ex(cls, parent, declared_object):
        # Get details of partition column from parent table
        partition_column = parent.get_child('columns', declared_object.column)
        # Validate partition column is time-based
        if partition_column.data_type not in {'datetime', 'datetime2'}:
            raise ValueError('Can only create partition on datetime column, not: {}'.format(partition_column.data_type))
        # Get partition boundary values (use existing data if present, otherwise create window around today)
        existing_min_value = parent.ex(sql.min_column_value.format(schema=parent.parent.name,
                                                                   table=parent.name,
                                                                   column=partition_column.name)).fetchone()[0]
        if existing_min_value:
            # Table contains existing data
            existing_max_value = parent.ex(sql.max_column_value.format(schema=parent.parent.name,
                                                                       table=parent.name,
                                                                       column=partition_column.name)).fetchone()[0]
            # Build partition boundary values to emcompass existing data
            boundary_start_date = existing_min_value.date() - timedelta(days=5)
            boundary_end_date = existing_max_value.date() + timedelta(days=5)
        else:
            # No existing data, create partition range around today
            boundary_start_date = date.today() - timedelta(days=5)
            boundary_end_date = date.today() + timedelta(days=5)

        boundary_values_str = ', '.join([(boundary_start_date + timedelta(days=i)).strftime("'%Y%m%d'")
                                         for i in range((boundary_end_date - boundary_start_date).days)])
        # Create partition function
        pf_name = 'pf_{}_{}_{}'.format(parent.parent.name, parent.name, partition_column.name)
        parent.ex(sql.CREATE_PARTITION_FUNCTION.format(pf_name=pf_name,
                                                       column_type=partition_column.data_type_representation(),
                                                       boundary_values=boundary_values_str))
        # Create partition scheme
        ps_name = 'ps_{}_{}_{}'.format(parent.parent.name, parent.name, partition_column.name)

        parent.ex(sql.CREATE_PARTITION_SCHEME.format(ps_name=ps_name,
                                                     pf_name=pf_name))
        parent.cur.commit()
        # Re-build indexes on partition scheme
        parent.recreate_indexes_on_filegroup('{}({})'.format(ps_name, partition_column.name))
        # Set name of declared object so creation verification passes
        declared_object.name = ps_name

    def _get_details_ex(self):
        return self.ex(sql.table_partition_details_for_scheme.format(schema=self.parent.parent.name,
                                                                     table=self.parent.name,
                                                                     ps_name=self.name)).fetchone()

    def get_attr_column(self, detail):
        return detail.column_name.lower()

    def _delete_ex(self):
        # Re-create indexes on PRIMARY filegroup
        self.parent.recreate_indexes_on_filegroup('[PRIMARY]')
        # Delete partition scheme
        self.ex(sql.DROP_PARTITION_SCHEME.format(ps_name=self.name))
        # Delete partition function
        pf_name = 'pf_{}_{}_{}'.format(self.parent.parent.name, self.parent.name, self.column)
        self.ex(sql.DROP_PARTITION_FUNCTION.format(pf_name=pf_name))
        self.cur.commit()

    def equate_declared(self, declared_obj):
        """
        Equate Partition on which column is involved, not name
        :param declared_obj:
        :return:
        """
        return self.column == declared_obj.column

    def get_function_name(self):
        """
        Get name of partition function for scheme
        :return:
        """
        return self.ex(sql.partition_function_for_scheme.format(ps_name=self.name)).fetchone().name

    def get_boundary_values(self):
        """
        Get range values of partition function
        :return:
        """
        return [boundary.value for boundary in self.ex(sql.PARTITION_RANGE_VALUES.format(ps_name=self.name)).fetchall()]

    def get_number_for_value(self, partition_value):
        """
        Get the partition number corresponding to value
        :param partition_value:
        :return:
        """
        return self.ex(sql.PARITION_NUMBER_FOR_VALUE.format(pf_name=self.get_function_name(),
                                                            value=partition_value)).fetchone().number

    def extend_range(self, to_date):
        """
        Extend boundary values of partition function until specified date
        :param date to_date:
        :return:
        """
        # Get current maximum boundary value
        max_boundary_value = self.get_boundary_values()[-1].date()
        if to_date > max_boundary_value:
            new_boundary_values = [max_boundary_value + timedelta(days=i + 1) for i in
                                   range((to_date - max_boundary_value).days)]
            pf_name = self.get_function_name()
            for boundary_date in new_boundary_values:
                log.info('Adding new partition function boundary value: {}'.format(boundary_date))
                self.ex(sql.SET_PARTITION_NEXT_FILEGROUP.format(scheme_name=self.name))
                self.ex(sql.SPLIT_PARITION_RANGE.format(fn_name=pf_name, new_date=boundary_date), commit=True)

    def merge_unitl_date(self, until_date):
        """
        Merge older partition boundary values until specified date
        :param date until_date:
        :return:
        """
        assert isinstance(until_date, date), 'Provided merge value must be a date'
        boundary_values = self.get_boundary_values()
        pf_name = self.get_function_name()
        for boundary_value in boundary_values:
            if boundary_value.date() <= until_date:
                log.info('Merging partition function boundary value: {}'.format(boundary_value))
                self.ex(sql.MERGE_PARTITION_RANGE.format(fn_name=pf_name, merge_date=boundary_value))
            else:
                # Exit once until_date is exceeded
                break


###########################################################################################
#       TABLE
###########################################################################################
class ReflectedTable(ReflectedEntity):
    object_type = 'tables'
    child_types = (ReflectedPrimaryKey, ReflectedIndex, ReflectedColumn, ReflectedPartition)

    @classmethod
    def _list_names_ex(cls, parent):
        return [result.name for result in
                parent.ex(sql.list_tables_in_schema.format(schema=parent.name)).fetchall()]

    @classmethod
    def _name_exists_ex(cls, parent, name):
        return bool(
            parent.ex(sql.TABLE_EXISTS.format(db_name=parent.parent.name, schema=parent.name, table=name)).fetchone())

    @classmethod
    def _create_ex(cls, parent, declared_object):
        # Get column list
        defined_columns = declared_object.get_children('columns')
        assert defined_columns, 'Cannot create table: {}, no columns have been defined'.format(declared_object)
        columns_sql = [column.sql_representation() for column in defined_columns]
        parent.ex(
            sql.CREATE_TABLE.format(schema=parent.name, table=declared_object.name, columns=', '.join(columns_sql)))

    def _get_details_ex(self):
        return self.ex(sql.table_details.format(schema=self.parent.name,
                                                table=self.name)).fetchone()

    def set_identity_insert(self, value):
        self.ex(sql.set_identity_insert.format(db_name=self.parent.parent.name,
                                               schema=self.parent.name,
                                               table=self.name,
                                               value='ON' if value else 'OFF'))

    def get_indexes_for_column(self, column_name):
        """
        Get all indexes that involve specified column
        :param str column_name:
        :return:
        """
        return [index for index in self.get_children('indexes') if index.includes_column(column_name)]

    def recreate_indexes_on_filegroup(self, filegroup):
        # Re-create clustered index on  filegroup
        self.get_clustered_index().recreate_new_filegroup(filegroup)
        # Re-build other indexes on filegroup
        for index in self.get_nonclustered_indexes():
            index.recreate_new_filegroup(filegroup)
        self.cur.commit()

    def get_clustered_index(self):
        for index in self.get_children('indexes'):
            if index.clustered:
                return index
        return None

    def get_nonclustered_indexes(self):
        return [index for index in self.get_children('indexes') if not index.clustered]

    def get_pk_fields(self):
        pks = self.get_children('primary_keys')
        if pks:
            return pks[0].columns

        else:
            return None

    def get_compression(self):
        """
        Get compression type of table (or clustered index of table)
        :return: NONE, ROW or PAGE
        """
        return self.ex(
            sql.table_compression_details.format(db_name=self.parent.parent.name, schema=self.parent.name,
                                                 table_name=self.name)).fetchone().compression

    def set_compression(self, compression_type, online=False):
        """
        Rebuild table with compression
        :param compression_type: NONE, ROW or PAGE
        :param bool online: Whether to rebuild table in ONLINE mode (allows access but takes longer)
        :return:
        """
        if compression_type not in {'PAGE', 'ROW', 'NONE'}:
            raise ValueError('Invalid compression type: {}'.format(compression_type))
        self.ex(sql.SET_TABLE_COMPRESSION.format(schema=self.parent.name, table=self.name,
                                                 compression=compression_type,
                                                 online='ON' if online else 'OFF'))

    def _rename_ex(self, new_name):
        self.ex(sql.RENAME_TABLE.format(schema=self.parent.name, old_name=self.name, new_name=new_name))

    def _delete_ex(self):
        self.ex(sql.DROP_TABLE.format(schema=self.parent.name, table=self.name))

    def has_data(self):
        return self.ex(sql.table_has_data.format(schema=self.parent.name, table=self.name)).fetchone()

    def clear_data(self):
        return self.ex(sql.DELETE_DATA.format(schema=self.parent.name, table=self.name)).rowcount

    def truncate_partitions(self, start_partition, end_partition):
        """
        Truncate data in table from given partitions
        :return:
        """
        return self.ex(sql.TRUNCATE_TABLE_PARTITIONS.format(schema=self.parent.name,
                                                            table=self.name,
                                                            start_partition=start_partition,
                                                            end_partition=end_partition))

    def __str__(self):
        return '[{}].[{}]'.format(self.parent.name, self.name)


###########################################################################################
#       SCHEMA
###########################################################################################
class ReflectedSchema(ReflectedEntity):
    object_type = 'schemas'
    child_types = (ReflectedTable,)
    system_names = ('sys', 'guest', 'INFORMATION_SCHEMA')

    @classmethod
    def _create_ex(cls, parent, declared):
        parent.ex(sql.CREATE_SCHEMA.format(schema_name=declared.name))

    @classmethod
    def _name_exists_ex(cls, parent, name):
        return bool(parent.ex(sql.SCHEMA_EXISTS.format(schema=name)).fetchone())

    @classmethod
    def _list_names_ex(cls, parent):
        return [result.name for result in parent.ex(sql.LIST_SCHEMAS.format(db_name=parent.name)).fetchall()]

    def _rename_ex(self, new_name):
        raise exceptions.DBError('Renaming schema not yet supported, have to create new schema and move all objects..')

    def _delete_ex(self):
        raise exceptions.DBError('Deleting schema not yet supported, have to delete all child objects..')


###########################################################################################
#       USER
###########################################################################################
class ReflectedUser(ReflectedEntity):
    object_type = 'users'
    ALL_DB_ROLES = {'db_accessadmin', 'db_datareader', 'db_datawriter', 'db_owner', 'db_securityadmin'}
    system_names = {'dbo', 'guest', 'sys', 'INFORMATION_SCHEMA'}

    @classmethod
    def from_declared(cls, parent, declared_obj, **kwargs):
        # See if user exists for declared user login
        login_name = declared_obj.login_name
        login_user = parent.ex(sql.USER_FOR_LOGIN.format(login_name=login_name)).fetchone()
        if login_user:
            log.debug('Found existing user: {} for login: {}'.format(login_user.name, login_user.login_name))
            return cls(parent, name=login_user.name)
        raise exceptions.DBObjectDoesntExistError('Could not find any existing user for login: {}'.format(login_name))

    @classmethod
    def _create_ex(cls, parent, declared):
        # Create user
        login_name = declared.login_name
        user_name = declared.name or login_name
        parent.ex(sql.CREATE_USER.format(user_name=user_name, login_name=login_name))
        # Add to roles
        for role_name in declared.db_roles:
            parent.ex(sql.alter_db_role.format(role=role_name, action='ADD', user_name=user_name), commit=True)

    @classmethod
    def _list_names_ex(cls, parent):
        return [result.name for result in parent.ex(sql.LIST_USERS).fetchall()]

    @classmethod
    def _name_exists_ex(cls, parent, name):
        return bool(parent.ex(sql.USER_EXISTS.format(user_name=name)).fetchone())

    def _get_details_ex(self):
        return self.ex(sql.USER_DETAIL.format(user_name=self.name)).fetchone()

    def get_attr_db_roles(self, detail):
        return set([result.role_name for result in self.ex(sql.get_user_roles.format(user_name=self.name))])

    def set_attr_db_roles(self, declared_obj):
        # First check if user login is owner of database
        if self.ex(sql.DATABASE_DETAIL.format(db_name=self.parent.name)).fetchone().owner == self.login_name:
            log.info('User login already owner of database, cant set roles')
            return
        # Roles to add
        for role_name in set(declared_obj.db_roles) - set(self.db_roles):
            self.ex(sql.alter_db_role.format(role=role_name, action='ADD', user_name=self.name), commit=True)
        # Roles to remove
        for role_name in set(self.db_roles) - set(declared_obj.db_roles):
            self.ex(sql.alter_db_role.format(role=role_name, action='DROP', user_name=self.name), commit=True)

    def equate_declared(self, declared_obj):
        """
        Equate uses on which login they are associated with, not name
        :param declared_obj:
        :return:
        """
        return self.login_name == declared_obj.login_name

    def _delete_ex(self):
        self.ex(sql.DROP_USER.format(user_name=self.name))


###########################################################################################
#       DATABASE
###########################################################################################
class ReflectedDatabase(ReflectedEntity):
    object_type = 'databases'
    child_types = (ReflectedSchema, ReflectedUser)
    system_names = ('master', 'tempdb', 'model', 'msdb', 'ReportServer', 'ReportServerTempDB')

    def extra_init(self):
        """
        Set current database
        :return:
        """
        self.cur.execute(f"USE [{self.name}]")

    @classmethod
    def _create_ex(cls, parent, declared_db):
        parent.ex(sql.CREATE_DB.format(db_name=declared_db.name,
                                       data_size=declared_db.data_size,
                                       log_size=declared_db.log_size,
                                       data_file_path=declared_db.data_file_path,
                                       log_file_path=declared_db.log_file_path), autocommit=True)

    @classmethod
    def _list_names_ex(cls, parent):
        return [result.name for result in parent.ex(sql.LIST_DATABASES).fetchall()]

    @classmethod
    def _name_exists_ex(cls, parent, name):
        return bool(parent.ex(sql.DB_EXISTS.format(db_name=name)).fetchone())

    def _get_details_ex(self):
        return self.ex(sql.DATABASE_DETAIL.format(db_name=self.name)).fetchone()

    def get_attr_data_size(self, detail):
        ds = self.ex(sql.db_sizes.format(db_name=self.name)).fetchone().row_size_mb
        # May get None result if lacking permissions or something? on TIAB DB
        return int(ds) if ds else 0

    def get_attr_log_size(self, detail):
        ls = self.ex(sql.db_sizes.format(db_name=self.name)).fetchone().log_size_mb
        # May get None result if lacking permissions or something? on TIAB DB
        return int(ls) if ls else 0

    def get_attr_data_file_path(self, detail):
        return self.ex(sql.db_file_info.format(db_name=self.name, file_type="ROWS")).fetchone().physical_name

    def get_attr_log_file_path(self, detail):
        return self.ex(sql.db_file_info.format(db_name=self.name, file_type="LOG")).fetchone().physical_name

    def set_attr_recovery_model_desc(self, declared_db):
        self.ex(sql.db_option_set.format(db_name=self.name, option_name='RECOVERY',
                                         value=declared_db.recovery_model_desc), autocommit=True)

    def set_attr_owner(self, declared_db):
        self.ex(sql.set_db_owner.format(db_owner=declared_db.owner), autocommit=True)

    def set_attr_data_file_path(self, declared_db):
        self.change_db_file_path('ROWS', declared_db.data_file_path)

    def set_attr_log_file_path(self, declared_db):
        self.change_db_file_path('LOG', declared_db.log_file_path)

    def change_db_file_path(self, file_type, new_path):
        """
        Helper method for changing database file path
        Will require user to manually move file
        :param str file_type: ROWS or LOG
        :param str new_path: New file path
        :return:
        """
        if self.ex(sql.DB_IN_HAG.format(db_name=self.name)).fetchone():
            raise Exception('Cannot change DB file path because it is involved in an Availability Group')
        if input(
                'Database must not be in use and you will need to manually move database file, do you want to continue? (y/n)') == 'y':
            # Get database file details
            file_info = self.ex(sql.db_file_info.format(db_name=self.name, file_type=file_type)).fetchone()
            # Change configured database
            self.cur.execute("USE [master]")
            with util.AutoCommit(self.cur.conn):
                self.ex(sql.CHANGE_DB_FILE_PATH.format(db_name=self.name,
                                                       file_name=file_info.name,
                                                       file_path=new_path), enforce_db=False)
                # Set database offline
                self.ex(sql.SET_DB_OFFLINE.format(db_name=self.name), enforce_db=False)
                input('Move file: {} to new location: {} then press Enter to continue'.format(file_info.physical_name,
                                                                                              new_path))
                # Set database online
                self.ex(sql.SET_DB_ONLINE.format(db_name=self.name), enforce_db=False)

            self.cur.execute(f"USE [{self.name}]")

    def set_db_file_size(self, file_type, new_size):
        """
        Helper method for changing database file size
        :param str file_type: Type of database file (ROWS or LOG)
        :param int new_size: New size for database file
        :return:
        """
        # Get current log size details
        file_detail = self.ex(sql.db_file_info.format(db_name=self.name, file_type=file_type)).fetchone()
        # Shrink  file
        if new_size < file_detail.current_size_mb:
            self.ex(sql.SHRINK_DB_FILE.format(file_name=file_detail.name, size=new_size), autocommit=True)
        # Grow  file
        else:
            self.ex(sql.GROW_DB_FILE.format(db_name=self.name,
                                            file_name=file_detail.name,
                                            size=new_size), autocommit=True)

    def get_tables(self, schema=None, **kwargs):
        """"Shortcut method to get all tables within database by getting intermediate schemas"""
        if schema:
            schemas = [self.get_child('schemas', schema)]
        else:
            schemas = self.get_children('schemas')
        schema_tables = [schema.get_children('tables', **kwargs) for schema in schemas]
        return [table for sublist in schema_tables for table in sublist]

    def get_table(self, table_name, schema_name='dbo', **kwargs):
        """Shortcut method which also gets an intermediate schema"""
        schema = self.get_child('schemas', schema_name, **kwargs)
        return schema.get_child('tables', table_name, **kwargs)

    def get_max_lsn(self):
        """
        Get maximum Log Sequence Number of database
        :return:
        """
        return self.ex(sql.CDC_MAX_LSN).fetchone()[0]

    def __str__(self):
        return self.name.upper()

    def can_delete(self):
        """Define condition for whether this object should be removed automatically if not declared"""
        return False

    def _rename_ex(self, new_name):
        with util.AutoCommit(self.cur.conn):
            self.ex(sql.RENAME_DATABASE.format(old_name=self.name, new_name=new_name))


###########################################################################################
#       LOGIN
###########################################################################################
class ReflectedLogin(ReflectedEntity):
    object_type = 'logins'
    ALL_SERVER_ROLES = {'sysadmin', 'securityadmin', 'serveradmin', 'setupadmin', 'processadmin', 'diskadmin',
                        'dbcreator', 'bulkadmin'}
    system_names = ('##MS_PolicyTsqlExecutionLogin##', 'GCT', '##MS_PolicyEventProcessingLogin##', 'sa')

    can_create = False

    @classmethod
    def _create_ex(cls, parent, declared_login):
        # Get password from credential
        parent.ex(sql.CREATE_LOGIN.format(login_name=declared_login.name,
                                          password=declared_login.password))

    @classmethod
    def _name_exists_ex(cls, parent, name):
        return bool(parent.ex(sql.LOGIN_EXISTS.format(login_name=name)).fetchone())

    @classmethod
    def _list_names_ex(cls, parent):
        return [result.name for result in parent.ex(sql.LIST_LOGINS).fetchall()]

    def get_attr_server_roles(self, detail_result):
        return set([role for role in self.ALL_SERVER_ROLES if getattr(detail_result, role) == 1])

    def set_attr_server_roles(self, declared_server):
        # Roles to add
        for role_name in set(declared_server.server_roles) - set(self.server_roles):
            self.ex(sql.ALTER_SERVER_ROLE.format(role=role_name, action='ADD', login_name=self.name), commit=True)
        # Roles to remove
        for role_name in set(self.server_roles) - set(declared_server.server_roles):
            self.ex(sql.ALTER_SERVER_ROLE.format(role=role_name, action='DROP', login_name=self.name), commit=True)

    def get_attr_password(self, detail):
        return 'dummypassword'

    def _get_details_ex(self):
        return self.ex(sql.LOGIN_DETAIL.format(name=self.name)).fetchone()

    def can_delete(self):
        """Define condition for whether this object should be removed automatically if not declared"""
        return True

    def _delete_ex(self):
        self.ex(sql.DELETE_LOGIN.format(login_name=self.name))


###########################################################################################
#       SERVER
###########################################################################################
class ReflectedServer(ReflectedEntity):
    """
    Object representing SLQServer Server Instance
    """
    object_type = 'servers'
    child_types = (ReflectedLogin, ReflectedDatabase)

    @classmethod
    def from_cursor(cls, cur):
        server_name = cur.execute('SELECT @@SERVERNAME').fetchone()[0]
        return cls(parent=None, cur=cur, name=server_name)

    def get_current_database(self):
        """
        Get database object for currently connected database
        :return:
        """
        db_name = self.cur.execute('SELECT DB_NAME() AS db_name').fetchone().db_name
        db = self.get_child('databases', db_name)
        if not db:
            raise Exception('Database: "{}" does not exist on server or login does not have access'.format(db_name))
        return db
