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
source_target_mapping_csv = os.path.join(parent_dir, 'input', 'source_target_mappings', 'hub_mapping.csv')
table_column_csv = os.path.join(parent_dir, 'input', 'table_definitions', 'tab_col.csv')
# output dir
output_file = os.path.join(parent_dir, 'create_hubs.py')

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


def datatype_from_list(tab_col_dict):
    datatype = 'UNKNOWN'
    if tab_col_dict['DATA_TYPE'] == 'TEXT':
        if tab_col_dict['CHARACTER_MAXIMUM_LENGTH']:
            datatype = 'String(' + tab_col_dict['CHARACTER_MAXIMUM_LENGTH'] + ')'
        else:
            datatype = 'String'
    elif tab_col_dict['DATA_TYPE'] == 'NUMBER':
        if tab_col_dict['NUMERIC_PRECISION']:
            datatype = 'Numeric(' + tab_col_dict['NUMERIC_PRECISION'] + "," + tab_col_dict['NUMERIC_SCALE'] + ")"
        else:
            datatype = 'Numeric'
    return datatype


datatype = {}
with open(source_target_mapping_csv, 'r') as hub_mapping_file:
    with open(table_column_csv, 'r') as tab_col_file:
        hub_rows = DictReader(hub_mapping_file)
        for hub_row in hub_rows:
            tabcol_rows = DictReader(tab_col_file)
            for tabcol_row in tabcol_rows:
                buskeys = hub_row['HUB_BUSINESS_KEY_DEFINITION'].split('.')
                for buskey in buskeys:
                    if hub_row['HUB_NAME'] == "HUB_" + tabcol_row['TABLE_NAME'] and hub_row['HUB_BUSINESS_KEY_DEFINITION'] == tabcol_row['COLUMN_NAME']:
                        del tabcol_row['TABLE_NAME']
                        del tabcol_row['COLUMN_NAME']
                        del tabcol_row['IS_NULLABLE']
                        datatype[hub_row['HUB_BUSINESS_KEY_DEFINITION']] = datatype_from_list(tabcol_row)


with open(output_file, 'w') as ofile:
    ofile.truncate()

with open(output_file, 'a') as ofile:
    ofile.write(create_hubs_intro)
    with open(source_target_mapping_csv, 'r') as ifile:
        rows = DictReader(ifile)
        table_sqlalch = template.render(rows=rows, datatype=datatype)
        ofile.write(table_sqlalch)
    ofile.write(create_hubs_outro)
