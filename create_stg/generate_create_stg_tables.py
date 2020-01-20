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
template = env.get_template('create_stage_tables.jinja2')

# find table definitions
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
tabdef_file_pattern = os.path.join(parent_dir, 'input', 'table_definitions', '*.csv')

# output dir
output_file = os.path.join(parent_dir, 'output', 'create_staging_tables.py')
create_staging_tables_intro = '''
from sqlalchemy import create_engine, MetaData, Table, Integer, String, Column, insert, sql, Sequence, exc
import configparser

# connect
config = configparser.ConfigParser()
config.read('foreign_key_snow/config.ini')

engine = create_engine(
    'snowflake://{user}:{password}@{account}/{database}/{schema}?warehouse={warehouse}'.format(
        user=config['db_credentials']['user'],
        password=config['db_credentials']['password'],
        account=config['db_connection']['account'],
        database=config['db_connection']['database'],
        schema=config['db_connection']['schema'],
        warehouse=config['db_connection']['warehouse']
    )
)

# results = connection.execute('select current_version()').fetchone()

def create_sample_tables(eng):
    # Some sample tables, without foreign keys
    metadata = MetaData()
'''
create_staging_tables_outro = '''
fertig jetzt
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

