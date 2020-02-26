import os
from jinja2 import FileSystemLoader, Environment
from csv import DictReader

# files
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# jinja templates
file_loader = FileSystemLoader(os.path.join(parent_dir, 'templates'))
env = Environment(loader=file_loader)
template = env.get_template('create_sat_metadata.jinja2')

table_column_csv = os.path.join(parent_dir, 'input', 'table_definitions', 'tab_col.csv')

# output dir
output_file = os.path.join(parent_dir, 'input', 'mappings', 'sat_mapping_generated.csv')


sample_metadata = '''
TABLE_NAME,COLUMN_NAME,IS_NULLABLE,DATA_TYPE,CHARACTER_MAXIMUM_LENGTH,NUMERIC_PRECISION,NUMERIC_SCALE
CUSTOMER,C_MKTSEGMENT,YES,TEXT,10,,
CUSTOMER,C_ACCTBAL,NO,NUMBER,,12,2
'''

with open(output_file, 'w') as ofile:
    ofile.truncate()
    with open(table_column_csv, 'r') as ifile:
        rows = DictReader(ifile)
        table_sqlalch = template.render(rows=rows)
        ofile.write(table_sqlalch)
