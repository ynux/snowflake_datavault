# Create a Data Vault from Metadata & Mapping, with Synthetic Data 

The idea for this project is to take some minimal input to build sqlalchemy code against Snowflake that creates and loads a data vault.

The input is:

* source table metadata (table definitions with columns and data type) 
* source to target mapping

The output is sqlalchemy scripts to create

* staging tables
* hubs
* sats
* links

and to load them.

### Sample Input Metadata and Data

This project uses Snowflake databases, and the sample database snowflake_sample_data.TPCH_SF1 .
It can either create "stupid synthetic data", simply random strings / numbers and some fixed dates, without any logic or structure. Or load sample data (see below for details). 

### What to use this for

Just practice, not real production - you should buy a tool.
The goal is to produce something visible, it takes many shortcuts. It only works under perfect conditions.

### Data Vault Metadata Design

The *metadata columns* of the sats and hubs are designed for [the TEAM metadata of Roelant Vos](https://github.com/RoelantVos/TEAM). For a minimal approach with the columns required by Data Vault 2.0 see [Kent Graziano's "Data Vault 2.0 Modeling Basics"](https://www.vertabelo.com/blog/data-vault-series-data-vault-2-0-modeling-basics/)

The *metadata for hub and sat generation* is a simplified version of what TEAM uses. For the full beauty see the github repository (`ClassJsonHandling.cs`).


### Notes on the Code

* written for python 3.7
* install the requirements
* for tests, there should be a sqlite version. See load_stg.py for a stub.

### How to use this

1. Prepare input metadata in `data/input/table_definitions.csv` (see below for examples)
2. Establish connection to Snowflake `bin/connect_snowflake.py` (everything in config.ini is expected to exist)

For synthetic data and simple data types (String without CHARACTER_MAXIMUM_LENGTH etc.):

3. generate create table statements for staging `bin/generate_create_stg_tables_simple.py`
4. run the create table script `./create_staging_tables_simple.py`
5. generate the load table statements for staging `bin/generate_load_stg_tables_simple.py`
6. Fill the staging tables with random data `./load_staging_tables_simple.py`

For sample data and full data types:

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

### Known Issues

* The code implicity relies on name conventions (names of hubs/links - their hash keys)
* the generated code should be named _generated and gitignored
* use try / except 
* missing cleanup scripts (truncate / drop tables in playgrounds)
* no standard for date and time format before md5 hashing, or encoding
