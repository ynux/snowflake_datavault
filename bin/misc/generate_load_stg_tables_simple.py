# This was an approach generating synthetic data rather than 
# using sample data. May still be useful later.
import os
import glob
from jinja2 import FileSystemLoader, Environment
from csv import DictReader

# for testing / to show csv structure
sample_csv_data = '''
tableA,colX,NUMBER,YES,,
tableA,colY,TEXT,NO,,
'''

# Text parts that only appear once
create_staging_tables_intro = '''
import random
import string
from datetime import timedelta, datetime
from sqlalchemy import create_engine, MetaData, Table


def randomString(stringLength=10):
    """Generate a random string of fixed length """
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(stringLength))


def get_next_day(thisday):
    return (datetime.strptime(thisday, "%Y-%m-%d") + timedelta(days=1)).date()


def generate_synth_data(table, base_rowid, count, base_date):
    synth_data = {}
'''
create_staging_tables_outro_1 = '''
    return synth_data


def insert_synth_data(eng, table_list):
    metadata2 = MetaData()
    connection = eng.connect()
    for table in table_list:
        table_lower = table.lower
        table_lower = Table(table, metadata2, autoload=True, autoload_with=eng)
        for count in range(4):
            sd = generate_synth_data(table, 337100, count, '2019-11-11')
            print(table, sd)
            if sd != {}:
                connection.execute(table_lower.insert(), sd)
        for count in range(4):
            sd = generate_synth_data(table, 337100, count, '2019-11-12')
            if sd != {}:
                connection.execute(table_lower.insert(), sd)
    connection.close()


if __name__ == "__main__":
    from bin import connect_snowflake
    import os
    basedir = os.path.dirname(os.path.abspath(__file__))
    configfile = os.path.join(basedir, 'conf', 'config.ini')
    engine = connect_snowflake.engine_snowflake(configfile, 'staging_db_schema')
'''
create_staging_tables_outro_2 = '''
    insert_synth_data(engine, table_list)
    engine.dispose()
'''

# jinja templates
file_loader = FileSystemLoader('./templates')
env = Environment(loader=file_loader)
template = env.get_template('load_stage_tables.jinja2')

# find table definitions
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
tabdef_file_pattern = os.path.join(parent_dir, 'input', 'table_definitions', '*.csv')

output_file = os.path.join(parent_dir, 'load_staging_tables_generated.py')

with open(output_file, 'w') as ofile:
    ofile.truncate()

with open(output_file, 'a') as ofile:
    ofile.write(create_staging_tables_intro)
    table_set = set()
    for matching_file in glob.glob(tabdef_file_pattern):
        with open(matching_file, 'r') as ifile:
            rows = DictReader(ifile, fieldnames=['table', 'column', 'data_type', 'nullable', 'comment'])
            table_sqlalch = template.render(rows=rows)
            ofile.write(table_sqlalch)
            ifile.seek(0)
            rows = DictReader(ifile, fieldnames=['table', 'column', 'data_type', 'nullable', 'comment'])
            for row in rows:
                table_set.add(row['table'])
    ofile.write(create_staging_tables_outro_1)
    ofile.write("    table_list = " + str(list(table_set)))
    ofile.write(create_staging_tables_outro_2)
