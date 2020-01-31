from sqlalchemy import insert, Table, MetaData
import os
from csv import DictReader
from bin import helpers

# files
present_dir = os.path.dirname(os.path.abspath(__file__))
# source-to-target-mapping
hub_mapping_csv = os.path.join(present_dir, 'input', 'mappings', 'hub_mapping.csv')
link_mapping_csv = os.path.join(present_dir, 'input', 'mappings', 'link_mapping.csv')

sample_hub_csv = '''
HUB_NAME,HUB_BUSINESS_KEY_DEFINITION,SOURCE_NAME
HUB_CUSTOMER,C_CUSTKEY,CUSTOMER
HUB_PARTSUPP,PS_PARTKEY.PS_SUPPKEY,PARTSUPP
'''

sample_link_csv = '''
LINK_NAME,HUB_NAMES
LNK_CUSTOMER_LINEITEM,HUB_CUSTOMER.HUB_LINEITEM
LNK_PART_LINEITEM,HUB_PART.HUB_LINEITEM
'''

def insert_hub_mappings(eng):
    metadata = MetaData()
    connection = eng.connect()
    hub_mappings = Table('HUB_MAPPINGS', metadata, autoload=True, autoload_with=eng)
    hub_business_keys = Table('HUB_BUSINESS_KEYS', metadata, autoload=True, autoload_with=eng)
    connection.execute(hub_mappings.delete())
    connection.execute(hub_business_keys.delete())

    with open(hub_mapping_csv, 'r') as ifile:
        hub_mapping_values = {}
        hub_business_keys_values = {}
        rows = DictReader(ifile)
        for row in rows:
            hub_mapping_values['HUB_NAME'] = row['HUB_NAME']
            hub_mapping_values['SOURCE_NAME'] = row['SOURCE_NAME']
            connection.execute(hub_mappings.insert().values([hub_mapping_values]))
            bkeydef = row['HUB_BUSINESS_KEY_DEFINITION']
            for bkey in bkeydef.split('.'):
                hub_business_keys_values['HUB_BUSINESS_KEY'] = bkey
                hub_business_keys_values['HUB_NAME'] = row['HUB_NAME']
                connection.execute(hub_business_keys.insert().values([hub_business_keys_values]))


def insert_link_mappings(eng):
    metadata = MetaData()
    connection = eng.connect()
    link_mappings = Table('LINK_MAPPINGS', metadata, autoload=True, autoload_with=eng)
    connection.execute(link_mappings.delete())

    with open(link_mapping_csv, 'r') as ifile:
        link_mapping_values = {}
        rows = DictReader(ifile)
        for row in rows:
            link_mapping_values['LINK_NAME'] = row['LINK_NAME']
            hubdef = row['HUB_NAMES']
            for hub in hubdef.split('.'):
                link_mapping_values['HUB_NAME'] = hub
                connection.execute(link_mappings.insert().values(link_mapping_values))


if __name__ == "__main__":
    # engine = helpers.engine_snowflake('metadata')
    engine = helpers.engine_sqlite('metadata')
    insert_hub_mappings(engine)
    insert_link_mappings(engine)
    engine.dispose()
