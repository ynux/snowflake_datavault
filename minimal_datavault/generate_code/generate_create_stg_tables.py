import os
from jinja2 import FileSystemLoader, Environment

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
output_file = os.path.join(parent_dir, 'create_staging_tables_generated.py')

# jinja templates
template_dir = os.path.join(parent_dir, 'templates')
file_loader = FileSystemLoader(template_dir)
env = Environment(loader=file_loader)
template_create_table = env.get_template('create_stage_tables_table.jinja2')
template_create_column = env.get_template('create_stage_tables_column.jinja2')


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
    engine = connect_snowflake.engine_snowflake(configfile, 'staging_db_schema')
    create_stage_tables(engine)
    engine.dispose()

'''


def write_create_staging_tables(metadata_rows):
    with open(output_file, 'w') as ofile:
        ofile.truncate()

    with open(output_file, 'a') as ofile:
        ofile.write(create_staging_tables_intro)
        table = ''
        first_table = True
        for row in metadata_rows:
            if row['TABLE_NAME'] != table:
                table_sqlalch_tab = template_create_table.render(row=row, first_table=first_table)
                first_table = False
                ofile.write(table_sqlalch_tab)
                table = row['TABLE_NAME']
            table_sqlalch_col = template_create_column.render(row=row)
            ofile.write(table_sqlalch_col)
        ofile.write(create_staging_tables_outro)
