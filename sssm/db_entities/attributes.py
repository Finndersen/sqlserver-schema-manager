valid_attributes = {
    'logins': ('type_desc', 'server_roles', 'password'),
    'databases': ('recovery_model_desc', 'data_size', 'log_size', 'owner', 'data_file_path', 'log_file_path'),
    'columns': ('data_type', 'char_max_len', 'datetime_precision', 'numeric_precision', 'numeric_scale', 'nullable', 'identity'),
    'foreign_keys': ('foreign_schema', 'foreign_table', 'foreign_column', 'column'),
    'primary_keys': ('columns', 'clustered', 'compression'),
    'indexes': ('columns', 'clustered', 'compression', 'included_columns', 'unique'),
    'users': ('login_name', 'db_roles',),
    'partitions': ('column',),
    'tables': [],
    'schemas': [],
    'servers': []
}