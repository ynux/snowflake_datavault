import os
from jinja2 import FileSystemLoader, Environment
from csv import DictReader

# files
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# jinja templates
file_loader = FileSystemLoader(os.path.join(parent_dir, 'templates'))
env = Environment(loader=file_loader)
template = env.get_template('create_hubs.jinja2')

# source-to-target-mapping
source_target_mapping_csv = os.path.join(parent_dir, 'input', 'mappings', 'hub_mapping.csv')
table_column_csv = os.path.join(parent_dir, 'input', 'table_definitions', 'tab_col.csv')
output_file = os.path.join(parent_dir, 'create_hubs_generated.py')

sample_metadata = '''
HUB_NAME,HUB_BUSINESS_KEY_DEFINITION,SOURCE_NAME,SOURCE_BUSINESS_KEY_DEFINITION
HUB_CUSTOMER,C_CUSTKEY:Numeric(38,0),CUSTOMER,simple
'''
create_hubs_intro = '''
from sqlalchemy import MetaData, Table, Numeric, String, Column, Boolean, Date


def create_hubs(eng):
    metadata = MetaData()
'''
create_hubs_outro = '''
    
    metadata.create_all(eng)


if __name__ == "__main__":
    from bin import connect_snowflake
    import os
    basedir = os.path.dirname(os.path.abspath(__file__))
    configfile = os.path.join(basedir, 'conf', 'config.ini')
    engine = connect_snowflake.engine_snowflake(configfile, 'rawvault_db_schema')
    create_hubs(engine)
    engine.dispose()

'''


with open(output_file, 'w') as ofile:
    ofile.truncate()

with open(output_file, 'a') as ofile:
    ofile.write(create_hubs_intro)
    with open(source_target_mapping_csv, 'r') as ifile:
        rows = DictReader(ifile)
        table_sqlalch = template.render(rows=rows)
        ofile.write(table_sqlalch)
    ofile.write(create_hubs_outro)
