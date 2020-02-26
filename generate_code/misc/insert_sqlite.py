from sqlalchemy import MetaData, Table, String, Column, select, inspect, create_engine, Index, Integer

engine_source = create_engine('sqlite:///sqlite3_src.db')
engine_target = create_engine('sqlite:///sqlite3_tgt.db')
metadata = MetaData()
# prepare tables
users = Table('USERS', metadata,
    Column('id', Integer, primary_key=True),
    Column('name', String),
    Column('fullname', String),
)
metadata.create_all(engine_source)
metadata.create_all(engine_target)

# put data into src
ins = users.insert().values(name='jacky', fullname='Jackie Jones')
# ins = users.insert().values(name='terry', fullname='Terry Jones')
conn_src = engine_source.connect()
conn_src.execute(ins)

# get columns
names = inspect(users).columns.keys()
conn_tgt = engine_target.connect()
conn_tgt.execute(users.delete())
select_stmt = select([users]).limit(3)
results = conn_src.execute(select_stmt)
for result in results:
    insert_stmt = users.insert(result)
    conn_tgt.execute(insert_stmt)

engine_source.dispose()
engine_target.dispose()

