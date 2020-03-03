# Create a Minimal Data Vault from Metadata & Mapping, using Snowflake Sample Data 

The idea of this project is to build a minimal raw data vault on top of a very simple Snwoflake Sample Database. It helped me to better understand which input, decisions and work building a raw vault involves. 

To run it, you need access to the Snowflake Sample Database snowflake_sample_data.TPCH_SF1, and be able to create 3 schemas (staging, metadata, dv_rav). There is an alternative SQLite version.

Many shortcuts are taken, and the code only works under perfect conditions. The goal is to produce something visible with the least possible effort. It is highly unsophisticated and definitely not meant for real life usage. The best it can hope for is making a more informed decision when building or chosing the real thing. 

## The Input:

* the sample database (table and column definitions, and some data), or the sqlite "source" and "metadata" databases
* source to target mapping for the hubs and links, manual

The output is sqlalchemy scripts or SQL to create and load

* staging tables
* hubs
* sats
* links

## Design Decisions: Data Model

* only use Data Vault 2.0 required columns in hubs / sats / links (no columns like an ETL_RUN_PID to support the ETL tool. For an example for such columns, see Roelant Vos's posts.)
* create one hub and satellite per staging table (no splitting of satellites or the like)
* no fancy data vault entities (self links, business vault, pit-tables, bridge tables)
* no data in links ("link satellites")
* hash merging for satellites (because the underlying staging data is stupid)

## Design Decisions: Code and Metadata

* put mapping metadata into a database
* rely on naming conventions (e.g. standard name for hub hash key)
* written for python 3.7
* install the requirements - mainly snowflake sqlalchemy and jinja. Using a virtualenv is recommended.
* have a sqlite version (see 
* sqlalchemy is abandoned when we get to the loading

General remark: There is a range of choices where to put meta information. You can have metadata, naming conventions or put information into tables, possibly into special columns. For the hash key, you can rely on naming convention, or put it into a metadata lookup table. For the source-to-target-naming, I don't think that a naming convention will work since you have no control over the source systems. Still, after the first load, the source will be in the tables, so you could drop the metadata. However, we want to easily recreate everything from the beginning, so the metadata is persisted in a small metadata database.

Why is the metadata in database tables and not in csv files? Because there will be joins to get the datatypes, and joining csv is ugly.

I aim at keeping the manually written mappings minimal. 

A note on the choice of business keys: In our simplistic example, the columns prefixed with `_key` are chosen as business keys. This part of the mapping could be done automatically in this case. Also, if the source has primary keys defined in the metadata, those could be used. However, this would again not work if you decided to use a business key that isn't the source system's primary key. This is highly recommended because it gives you the freedom to switch the source, which probably was the reason why you chose the data vault model in the first case. Overall, it is very realistic to have humans decide the business key, so it is part of the manual mapping.

#### Metadata tables

SCHEMAS
HUB_MAPPINGS
HUB_BUSINESS_KEYS
LINK_MAPPINGS
COLUMN_DTYPES


## Sources

* Kent Graziano
* Dani Schnider
* Roelant Vos

## How to use this

Prepare environment: : Create virtualenv, e.g. `virtualenv env -p python3; source env/bin/activate`(Recommended but not strictly needed.) Install dependencies with `pip install -r requirement.txt`

using sqlite as target db, snowflake as source:

0. Prepare the config `./conf/config.ini`
1. Establish connection to Snowflake `python minimal_datavault/generate_code/helpers.py`
2. snowflake: Create schemas `python create_schemas.py`
3. Create and fill metadata tables: `python3 minimal_datavault/create_fill_metadata_tables.py`, `python3 minimal_datavault/insert_mappings.py`. The result is a sqlite db `sqlite_dbs/metadata.db` with filled tables COLUMN_DTYPES, HUB_MAPPINGS, SCHEMAS, HUB_BUSINESS_KEYS, LINK_MAPPINGS    


3. generate create table statements for staging `bin/generate_create_stg_tables_simple.py`
4. run the create table script `./create_staging_tables_simple.py`
5. generate the load table statements for staging `bin/generate_load_stg_tables_simple.py`
6. Fill the staging tables with random data `./load_staging_tables_simple.py`
3. generate create table statements for staging , and actually create them: `python bin/generate_create_stg_tables.py`,  `python ./create_staging_tables.py`
5. load the staging tables with sample data `./load_stg.py`
8. For the Hubs: Create a source-to-target mapping manually in `input/mappings/hub_mapping.csv`. Then generate statements to create hubs, and create them : `python bin/generate_create_hubs.py`, `python ./create_hubs.py`
8. For the Satellites: generate or write metadata to create sats, then generate statements to create sats. Or generate metadata and change it manually. As the third and last step, create the satellite tables: `python bin/generate_sat_metadata.py; python bin/generate_create_sats.py; python create_sats.py` 
9. Create link metadata, manually. As a simplification, only links connecting two hubs are expected, without own data (no link satellites).
9. Create links `./bin/generate_create_links.py; ./create_links_generated.py` 
10. Create load hubs statements: `./bin/generate_load_hubs.py`
11. Create load links statements: `./bin/generate_load_links.py`
12. Create load sats statements TODO
13. Load hubs - use sql in `load_hub_generated.py` (or write sqlalchemy code to run it)
14. Load links: use sql in `load_links_generated.py`
15. Load sats TODO

## How to get rid of this (in snowflake)

Remove schemas (metadata, raw vault, staging): `python drop_schemas.py`

#### SQLite version

The required data is in ... xxx

CSV version?

### Getting Sample Input Metadata

For the full column definition:
xxx superceded by sqlite thingy
```
use database snowflake_sample_data;

select col.table_name, col.column_name, col.is_nullable, col.data_type, col.CHARACTER_MAXIMUM_LENGTH, col.NUMERIC_PRECISION, col.NUMERIC_SCALE
from information_schema.columns col 
join information_schema.tables tab
on ( col.table_name = tab.table_name and col.table_schema = tab.table_schema)
where col.table_schema = 'TPCH_SF1'
and tab.table_type = 'BASE TABLE';
```
snowflake datatypes are so simple ... delightful. It's all DATE, NUMBER, TEXT.

### Remark on Snowflake's Sample DB

The sample schema has some special features. The data is apperently generated randomly, so for every where clause, one row comes back. Also, i couldn't query the information query for the objects.

There are now primary or foreign keys in the sample db, but keys have the suffix `_key`. For a diagram, see [Snowflake Documentation, Sample Data: TPC-H](https://docs.snowflake.net/manuals/user-guide/sample-data-tpch.html)


### Known Issues

* use try / except 
* missing cleanup scripts (truncate / drop tables in playgrounds)
* no standard for date and time format before md5 hashing, or encoding, or to-charing
* no satellite loading yet
* no foreign keys (hubs - sats, hubs - links)
