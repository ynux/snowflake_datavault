
from sqlalchemy import MetaData, Table, select


def create_sample_tables(eng_src, eng_tgt):
    metadata = MetaData()
    inspector = inspect(eng_src)
    table_names = inspector.get_table_names()

    for table_name in table_names:
        table = Table(table_name, metadata, autoload=True, autoload_with=eng_src)
        metadata.create_all(bind=eng_tgt, checkfirst=True)


def fill_sample_tables(eng_src, eng_tgt, num_rows):
    metadata = MetaData()
    inspector = inspect(eng_src)
    table_names = inspector.get_table_names()
    conn_tgt = eng_tgt.connect()
    conn_src = eng_src.connect()
    for table_name in table_names:
        table = Table(table_name, metadata, autoload=True, autoload_with=eng_src)
        conn_tgt.execute(table.delete())
        select_stmt = select([table]).limit(num_rows)
        results = conn_src.execute(select_stmt)
        for result in results:
            insert_stmt = table.insert(result)
            conn_tgt.execute(insert_stmt)


if __name__ == "__main__":
    from bin import helpers
    import os
    from sqlalchemy import create_engine, inspect
    basedir = os.path.dirname(os.path.abspath(__file__))
    configfile = os.path.join(basedir, 'conf', 'config.ini')
    engine_source = helpers.engine_snowflake('source')
    engine_target = create_engine('sqlite:///source.db')
    create_sample_tables(engine_source, engine_target)
    fill_sample_tables(engine_source, engine_target, 100)
    engine_source.dispose()
    engine_target.dispose()

