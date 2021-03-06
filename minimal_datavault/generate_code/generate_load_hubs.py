import os
from jinja2 import FileSystemLoader, Environment
from csv import DictReader

# files
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# jinja templates
file_loader = FileSystemLoader(os.path.join(parent_dir, 'templates'))
env = Environment(loader=file_loader)
template = env.get_template('load_hubs.jinja2')

# source-to-target-mapping
hub_mapping_csv = os.path.join(parent_dir, 'input', 'mappings', 'hub_mapping.csv')
# output dir
output_file = os.path.join(parent_dir, 'load_hub_generated.py')

sample_metadata = '''
HUB_SCHEMA_NAME,HUB_NAME,HUB_BUSINESS_KEY_DEFINITION,SOURCE_SCHEMA_NAME,SOURCE_NAME
DV_RAW,HUB_CUSTOMER,C_CUSTKEY,DV_STG,CUSTOMER
'''

with open(output_file, 'w') as ofile:
    ofile.truncate()

with open(output_file, 'a') as ofile:
    with open(hub_mapping_csv, 'r') as ifile:
        rows = DictReader(ifile)
        sql = template.render(rows=rows)
        ofile.write(sql)
