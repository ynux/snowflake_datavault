import os
from jinja2 import FileSystemLoader, Environment
from csv import DictReader

# files
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# jinja templates
file_loader = FileSystemLoader(os.path.join(parent_dir, 'templates'))
env = Environment(loader=file_loader)
template = env.get_template('load_links.jinja2')

# source-to-target-mapping
link_mapping_csv = os.path.join(parent_dir, 'input', 'mappings', 'link_mapping.csv')
# output dir
output_file = os.path.join(parent_dir, 'load_link_generated.py')

sample_metadata = '''
LINK_SCHEMA_NAME,LINK_NAME,FIRST_HUB,SECOND_HUB,FIRST_HUB_BKEYS,SECOND_HUB_BKEYS,SOURCE_SCHEMA_NAME
DV_RAV,LNK_CUSTOMER_LINEITEM,HUB_CUSTOMER,HUB_LINEITEM,C_CUSTKEY,PS_SUPPKEY,DV_STG
DV_RAV,LNK_REGION_SUPPLIER,HUB_REGION,HUB_SUPPLIER,R_REGIONKEY,S_REGIONKEY,DV_STG
'''

with open(output_file, 'w') as ofile:
    ofile.truncate()

with open(output_file, 'a') as ofile:
    with open(link_mapping_csv, 'r') as ifile:
        rows = DictReader(ifile)
        sql = template.render(rows=rows)
        ofile.write(sql)
