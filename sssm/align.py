from sssm import exceptions
from sssm.db_entities import attributes, reflected
import logging

log = logging.getLogger(__name__)


def align_server(cursor, declared_server):
    """
    Helper method to initiate database server alignment
    :param cursor: Database connection cursor
    :param declared_server: DeclaredServer instance
    :return:
    """
    refleted_server = reflected.ReflectedServer.from_cursor(cursor)
    align_entity(declared_server, refleted_server)


def align_entity(declared_obj, reflected_obj, align_children=True):
    """
    Align database state with DeclaredObject
    :param parent_declared:
    :param reflected_parent: ReflectedObject of parent object
    :param bool align_children: Whether or not to also align child objects
    :return:
    """
    log.debug('Aligning: {} with: {}'.format(declared_obj.display_details(), reflected_obj.display_details()))

    assert declared_obj.object_type == reflected_obj.object_type, 'Declared and reflected objects must be of same type to align'
    # Compare/update entity attributes
    for attr_name in attributes.valid_attributes[declared_obj.object_type]:
        if getattr(declared_obj, attr_name) != getattr(reflected_obj, attr_name):
            log.info('{} attribute "{}" is currently: "{}" but should be: "{}"'.format(reflected_obj, attr_name,
                                                                           getattr(reflected_obj, attr_name),
                                                                           getattr(declared_obj, attr_name)))
            reflected_obj.set_attribute(declared_obj, attr_name)
    # Align children
    if align_children:
        for child_type in declared_obj.child_types:
            align_child_type(declared_obj, reflected_obj, child_type)


def align_child_type(declared_parent, reflected_parent, child_class):
    """
    Perform database alignment of all children objects of particular type
    :param DeclaredObject declared_parent: Declared parent instance
    :param ReflectedObject reflected_parent: corresponding reflected database object of current declared object
    :param DeclaredObject child_class: Child type class
    :return:
    """
    log.debug('Aligning children "{}" of {}'.format(child_class.object_type, declared_parent))
    declared_children = declared_parent.get_children(child_class.object_type)
    if declared_children is not None:
        # Remove any existing children not declared
        if not declared_parent.ignore_extra_children_type(child_class.object_type):
            log.debug('Checking existing {} children of: {}'.format(child_class.object_type, reflected_parent))
            for existing_child in reflected_parent.get_children(child_class.object_type):
                log.debug('Found child object: {}'.format(existing_child))
                # Check whether object is in declaration
                if any((existing_child == declared_child for declared_child in declared_children)):
                    log.debug('Existing child object: {} matches declaration'.format(existing_child))
                    continue
                log.debug('Existing child: {} does not match any in definition: {}'.format(existing_child,
                                                                                           [declared.name for declared
                                                                                            in declared_children]))
                existing_child.delete()

        # Align declared children
        for declared_child in declared_children:
            # Rename existing  object with old_name
            if declared_child.old_name:
                try:
                    old_name_child = reflected_parent.get_child(declared_child.object_type, declared_child.old_name)
                    old_name_child.rename(declared_child.name)
                except exceptions.DBObjectDoesntExistError as e:
                    pass
            # Get reflected object from parent
            reflected_obj = reflected_parent.get_or_create_child(declared_child)
            if reflected_obj:
                # Align reflected with declared
                align_entity(declared_child, reflected_obj)
