import os
import glob
from jinja2 import FileSystemLoader, Environment
from csv import DictReader

# for testing
sample_csv_data = '''
tableA,colX,NUMBER,YES,,
tableA,colY,TEXT,NO,,
'''

# jinja templates
file_loader = FileSystemLoader('./templates')
env = Environment(loader=file_loader)
template = env.get_template('create_stage_tables_simple.jinja2')

# find table definitions
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
tabdef_file_pattern = os.path.join(parent_dir, 'input', 'table_definitions_simple', '*.csv')

output_file = os.path.join(parent_dir, 'create_staging_tables_generated.py')
create_staging_tables_intro = '''
from sqlalchemy import MetaData, Table, Integer, String, Column, Boolean


def create_stage_tables(eng):
    # Some sample tables, without foreign keys
    metadata = MetaData()
'''
create_staging_tables_outro = '''
    metadata.create_all(eng)


if __name__ == "__main__":
    from bin import connect_snowflake
    import os
    basedir = os.path.dirname(os.path.abspath(__file__))
    configfile = os.path.join(basedir, 'conf', 'config.ini')
    engine = connect_snowflake.engine_snowflake(configfile, 'staging_db_schema')
    create_stage_tables(engine)
    engine.dispose()

'''
with open(output_file, 'w') as ofile:
    ofile.truncate()

with open(output_file, 'a') as ofile:
    ofile.write(create_staging_tables_intro)
    for matching_file in glob.glob(tabdef_file_pattern):
        with open(matching_file, 'r') as ifile:
            rows = DictReader(ifile, fieldnames=['table', 'column', 'data_type', 'nullable', 'comment'])
            table_sqlalch = template.render(rows=rows)
            ofile.write(table_sqlalch)
    ofile.write(create_staging_tables_outro)

