import os
from jinja2 import FileSystemLoader, Environment
from csv import DictReader


# for testing
sample_csv_data = '''
SOURCE_NAME,TARGET_NAME,SOURCE_ATTRIBUTE_NAME:DATATYPE,TARGET_ATTRIBUTE_NAME:DATATYPE
CUSTOMER,SAT_CUSTOMER,"C_MKTSEGMENT:String(10)","C_MKTSEGMENT:String(10)"
CUSTOMER,SAT_CUSTOMER,"C_ACCTBAL:Numeric(12,2)","C_ACCTBAL:Numeric(12,2)"
'''

# jinja templates
file_loader = FileSystemLoader('./templates')
env = Environment(loader=file_loader)
template_create_table = env.get_template('create_sats_table.jinja2')
template_create_column = env.get_template('create_sats_column.jinja2')

# find table definitions
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sat_mapping_csv = os.path.join(parent_dir, 'input', 'mappings', 'sat_mapping_generated.csv')

output_file = os.path.join(parent_dir, 'create_sats_generated.py')
create_sats_intro = '''
from sqlalchemy import MetaData, Table, Numeric, String, Column, Boolean, Date


def create_sats(eng):
    metadata = MetaData()
'''
create_sats_outro = '''
    )
    metadata.create_all(eng)


if __name__ == "__main__":
    from bin import connect_snowflake
    import os
    basedir = os.path.dirname(os.path.abspath(__file__))
    configfile = os.path.join(basedir, 'conf', 'config.ini')
    engine = connect_snowflake.engine_snowflake(configfile, 'rawvault_db_schema')
    create_sats(engine)
    engine.dispose()

'''
with open(output_file, 'w') as ofile:
    ofile.truncate()

with open(output_file, 'a') as ofile:
    ofile.write(create_sats_intro)
    with open(sat_mapping_csv, 'r') as ifile:
        rows = DictReader(ifile)
        table = ''
        first_table = True
        for row in rows:
            if row['TARGET_NAME'] != table:
                table_sqlalch_tab = template_create_table.render(row=row, first_table=first_table)
                first_table = False
                ofile.write(table_sqlalch_tab)
                table = row['TARGET_NAME']
            table_sqlalch_col = template_create_column.render(row=row)
            ofile.write(table_sqlalch_col)
    ofile.write(create_sats_outro)

