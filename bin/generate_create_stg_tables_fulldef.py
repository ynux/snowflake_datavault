import os
from jinja2 import FileSystemLoader, Environment
from csv import DictReader


# for testing
sample_csv_data = '''
TABLE_NAME,COLUMN_NAME,IS_NULLABLE,DATA_TYPE,CHARACTER_MAXIMUM_LENGTH,NUMERIC_PRECISION,NUMERIC_SCALE
CUSTOMER,C_MKTSEGMENT,YES,TEXT,10,,
CUSTOMER,C_ACCTBAL,NO,NUMBER,,12,2
'''

# jinja templates
file_loader = FileSystemLoader('./templates')
env = Environment(loader=file_loader)
template_create_table = env.get_template('create_stage_tables_fulldef_table.jinja2')
template_create_column = env.get_template('create_stage_tables_fulldef_column.jinja2')

# find table definitions
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
table_column_csv = os.path.join(parent_dir, 'input', 'table_definitions_full', 'full_column_def.csv')

# output dir
output_file = os.path.join(parent_dir, 'create_staging_tables_fulldef.py')
create_staging_tables_intro = '''
from sqlalchemy import MetaData, Table, Numeric, String, Column, Boolean, Date


def create_stage_tables(eng):
    metadata = MetaData()
'''
create_staging_tables_outro = '''
    )
    metadata.create_all(eng)


if __name__ == "__main__":
    from bin import connect_snowflake
    import os
    basedir = os.path.dirname(os.path.abspath(__file__))
    configfile = os.path.join(basedir, 'conf', 'config.ini')
    engine = connect_snowflake.engine_snowflake(configfile)
    create_stage_tables(engine)
    engine.dispose()

'''
with open(output_file, 'w') as ofile:
    ofile.truncate()

with open(output_file, 'a') as ofile:
    ofile.write(create_staging_tables_intro)
    with open(table_column_csv, 'r') as ifile:
        rows = DictReader(ifile)
        table = ''
        first_table = True
        for row in rows:
            if row['TABLE_NAME'] != table:
                table_sqlalch_tab = template_create_table.render(row=row, first_table=first_table)
                first_table = False
                ofile.write(table_sqlalch_tab)
                table = row['TABLE_NAME']
            table_sqlalch_col = template_create_column.render(row=row)
            ofile.write(table_sqlalch_col)
    ofile.write(create_staging_tables_outro)

