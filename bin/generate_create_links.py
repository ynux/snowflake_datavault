import os
from jinja2 import FileSystemLoader, Environment
from csv import DictReader

# files
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# jinja templates
file_loader = FileSystemLoader(os.path.join(parent_dir, 'templates'))
env = Environment(loader=file_loader)
template = env.get_template('create_links.jinja2')

# source-to-target-mapping
link_mapping_csv = os.path.join(parent_dir, 'input', 'mappings', 'link_mapping.csv')
table_column_csv = os.path.join(parent_dir, 'input', 'table_definitions', 'tab_col.csv')
hub_mapping_csv = os.path.join(parent_dir, 'input', 'mappings', 'hub_mapping.csv')

output_file = os.path.join(parent_dir, 'create_links_generated.py')

sample_metadata = '''
[LINK_NAME],[FIRST_HUB_NAME],[SECOND_HUB_NAME]
LNK_CUSTOMER_LINEITEM,CUSTOMER,LINEITEM
'''
create_links_intro = '''
from sqlalchemy import MetaData, Table, Numeric, String, Column, Boolean, Date


def create_links(eng):
    metadata = MetaData()
'''
create_links_outro = '''
    
    metadata.create_all(eng)


if __name__ == "__main__":
    from bin import connect_snowflake
    import os
    basedir = os.path.dirname(os.path.abspath(__file__))
    configfile = os.path.join(basedir, 'conf', 'config.ini')
    engine = connect_snowflake.engine_snowflake(configfile, 'rawvault_db_schema')
    create_links(engine)
    engine.dispose()

'''


def lookup_hub_businesskey(hub_name, hubdef_csv):
    buskeys = []
    with open(hubdef_csv, 'r') as hub_mapping_file:
        hub_rows = DictReader(hub_mapping_file)
        for hub_row in hub_rows:
            if hub_row['HUB_NAME'] == hub_name:
                buskeys = hub_row['HUB_BUSINESS_KEY_DEFINITION'].split('.')
    return buskeys


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


def datatype_from_dict(tab_col_dict):
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


def get_hubkey_datatype(hubdef_csv):
    table_column_csv = os.path.join(parent_dir, 'input', 'table_definitions', 'tab_col.csv')
    datatype_dict = {}
    with open(hubdef_csv, 'r') as hub_mapping_file:
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
                            datatype_dict[hub_row['HUB_BUSINESS_KEY_DEFINITION']] = datatype_from_dict(tabcol_row)
    return datatype_dict


datatypes = get_hubkey_datatype(hub_mapping_csv)
buskeys = {}
with open(source_target_mapping_csv, 'r') as ifile:
    rows = DictReader(ifile)
    for row in rows:
        buskeys[row['FIRST_HUB']] = lookup_hub_businesskey(row['FIRST_HUB'], hub_mapping_csv)
        buskeys[row['SECOND_HUB']] = lookup_hub_businesskey(row['FIRST_HUB'], hub_mapping_csv)

with open(output_file, 'w') as ofile:
    ofile.truncate()

with open(output_file, 'a') as ofile:
    ofile.write(create_links_intro)
    with open(source_target_mapping_csv, 'r') as ifile:
        rows = DictReader(ifile)
        table_sqlalch = template.render(rows=rows)
        ofile.write(table_sqlalch)
    ofile.write(create_links_outro)
