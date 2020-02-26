import os
base = os.path.dirname(os.path.abspath(__file__))
base_parent = os.path.dirname(base)

input_data_dir = os.path.join(base_parent, 'input')
template_dir = os.path.join(base_parent, 'templates')
sqlite_db_dir = os.path.join(base_parent, 'sqlite_dbs')

