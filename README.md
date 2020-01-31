# Create a Data Vault from Metadata & Mapping, using Snwoflake Sample Data 

The idea of this project is to build a minimal raw data vault on top of a minimal Snwoflake Sample Database. It helped me to better understand which input, decisions and work building a raw vault involves. 

To run it, you need access to the Snowflake Sample Database snowflake_sample_data.TPCH_SF1, and be able to create 3 schemas (staging, metadata, dv_rav)

Many shortcuts are taking, and it only works under perfect conditions. The goal is to produce something visible with the least possible effort.

## The Input:

* source schema metadata (table definitions with columns and data type) and data, from the sample db
* source to target mapping, manual

The output is sqlalchemy scripts to create 

* staging tables
* hubs
* sats
* links

and sql to load them.

## Design Decisions: Data Model

* only use required columns (nothing e.g. like a ETL_RUN_PID to support the ETL tool)
* create one hub and satellite per staging table (no splitting of satellites or stuff)
* no fancy data vault entities (self links, business vault, pit-tables, bridge tables etc)
* no data in links ("link satellites")
* hash merging for satellites (because the underlying staging data is stupid)

## Design Decisions: Code

* put mapping metadata into a database (i would have liked to keep everything in csv, but some joining between mapping and table column metadata is needed)
* rely on naming conventions (e.g. standard name for hub hash key)
* written for python 3.7
* install the requirements - mainly snowflake sqlalchemy and jinja. Using a virtualenv is recommended.
* have a sqlite version (see 
* sqlalchemy is abandoned when we get to the loading

## Sources

* Kent Graziano
* Dani Schnider
* Roelant Vos

## How to use this

1. Establish connection to Snowflake `bin/connect_snowflake.py` 
2. Create schemas (./create_schemas.py)
3. Create mapping for hubs and links (manually)
4. Create and fill metadata tables
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

### Getting Sample Input Metadata

For the full column definition:

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

* The code implicity relies on name conventions (names of hubs/links - their hash keys)
* use try / except 
* missing cleanup scripts (truncate / drop tables in playgrounds)
* no standard for date and time format before md5 hashing, or encoding, or to-charing
* only very simple links 
* no satellite loading yet
* no foreign keys (hubs - sats, hubs - links)
