from sqlalchemy import insert, Table, MetaData
import os
from csv import DictReader
from bin import helpers

# files
present_dir = os.path.dirname(os.path.abspath(__file__))
# source-to-target-mapping
source_target_mapping_csv = os.path.join(present_dir, 'input', 'mappings', 'hub_mapping.csv')

sample_csv = '''
HUB_NAME,HUB_BUSINESS_KEY_DEFINITION,SOURCE_NAME
HUB_CUSTOMER,C_CUSTKEY,CUSTOMER
HUB_PARTSUPP,PS_PARTKEY.PS_SUPPKEY,PARTSUPP
'''


def insert_hub_mappings(eng):

    metadata = MetaData()
    connection = eng.connect()
    hub_mappings = Table('HUB_MAPPINGS', metadata, autoload=True, autoload_with=eng)
    connection.execute(hub_mappings.insert(), {'HUB_NAME': 'HUB_CUSTOMER', 'SOURCE_NAME': 'CUSTOMER'})
    hub_business_keys = Table('HUB_BUSINESS_KEYS', metadata, autoload=True, autoload_with=eng)

    with open(source_target_mapping_csv, 'r') as ifile:
        hub_mapping_values = {}
        hub_business_keys_values = {}
        rows = DictReader(ifile)
        for row in rows:
            hub_mapping_values['HUB_NAME'] = row['HUB_NAME']
            hub_mapping_values['SOURCE_NAME'] = row['SOURCE_NAME']
            print(hub_mapping_values)
            connection.execute(hub_mappings.insert().values([hub_mapping_values]))
            bkeydef = row['HUB_BUSINESS_KEY_DEFINITION']
            for bkey in bkeydef.split('.'):
                hub_business_keys_values['HUB_BUSINESS_KEY'] = bkey
                hub_business_keys_values['HUB_NAME'] = row['HUB_NAME']
                connection.execute(hub_business_keys.insert().values([hub_business_keys_values]))


if __name__ == "__main__":
    engine = helpers.engine_snowflake('metadata')
    insert_hub_mappings(engine)
    engine.dispose()

