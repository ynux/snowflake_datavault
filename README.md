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
It creates "stupid synthetic data", simply random strings / numbers and some fixed dates, without any logic or structure. It would be better to switch to the actual data in the sample database 

### What to use this for

Just practice, not real production - you should buy a tool.
The goal is to produce something visible, it takes many shortcuts.

### Data Vault Metadata Design

The metadata columns of the sats and hubs are designed for [the TEAM metadata of Roelant Vos](https://github.com/RoelantVos/TEAM)
The metadata for hub and sat generation is a simplified version of what TEAM uses.

### Notes on the Code

* written for python 3.7
* install the requirements
* for tests, there should be a sqlite version

### How to use this

1. Prepare input metadata in `data/input/table_definitions.csv` (see below for examples)
2. Establish connection to Snowflake `bin/connect_snowflake.py`
3. generate create table statements for staging `bin/generate_create_stg_tables.py`
   OR `bin/generate_create_stg_tables_full.py`
4. run the create table script `./create_staging_tables.py` OR `./create_staging_tables_full.py`
5. generate the load table statements for staging `bin/generate_load_stg_tales.py` (with synthetic data, simple data types) OR get real data (TODO)
6. Fill the staging tables with random data `./load_staging_tables.py` or sample data (TODO)
7. Create the source-to-target-mapping (hub & sat metadata), manually 
8. generate statements to create hubs, and create them : `bin/generate_create_hubs.py`, `./create_hubs.py`
8. generate metadata to create sats, generate statements to create sats. This is done in two steps in case a user wants to manually change the attribute mapping. Then create the satellite tables. (TODO)
9. Create link metadata, manually
9. Create links
10. Create load hubs statements
11. Create load links statements
12. Create load sats statements
13. Load hubs
14. Load links
15. Load sats

### Shortcuts Taken

* Presently, everything goes into the schema configured in the config.ini
* we upper case all tables and columns

### Getting Sample Input Metadata

```
use database snowflake_sample_data;

select col.table_name, column_name, data_type, is_nullable, col.comment 
from information_schema.columns col 
join information_schema.tables tab
on ( col.table_name = tab.table_name and col.table_schema = tab.table_schema)
where col.table_schema = 'TPCH_SF1'
and tab.table_type = 'BASE TABLE';

for table_name in $(cut -d, -f1 base_table_cols_snow_sample_db.csv | sort -u | grep -vw TABLE_NAME); do echo ${table_name}; csvgrep -c1 -m ${table_name} base_table_cols_snow_sample_db.csv > ${table_name}.csv; done

```
will create csv with BOM
unbomb it and cut it into pieces
remove header
(we could also change the code to go over the tables, this is just for now when you might want to add one table after the other)


```
cut -d, -f3 *.csv | sort -u
DATE
NUMBER
TEXT

```
snowflake is so nice & simple ... delightful
For the full column definition:

```
select col.table_name, col.column_name, col.is_nullable, col.data_type, col.CHARACTER_MAXIMUM_LENGTH, col.NUMERIC_PRECISION, col.NUMERIC_SCALE
from information_schema.columns col 
join information_schema.tables tab
on ( col.table_name = tab.table_name and col.table_schema = tab.table_schema)
where col.table_schema = 'TPCH_SF1'
and tab.table_type = 'BASE TABLE';
```


