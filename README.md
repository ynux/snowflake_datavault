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

### Sample metadata

As Sample table definition this project uses some of the tables in the Production Schema of Microsoft's [AdventureWorks sample data warehouse](https://github.com/microsoft/sql-server-samples/tree/master/samples/databases/adventure-works). The datatypes are simplified.

### What to use this for

Just practice, not real production - you should buy a tool.

### Metadata Design

The metadata is designed for [the TEAM metadata of Roelant Vos](https://github.com/RoelantVos/TEAM)

### Notes on the Code

written for python 3.7
install the requirements
for tests, there should be a sqlite version

### How to use this

1. Prepare input data in `input/table_definitions.csv`
2. Establish connection to Snowflake `helper/connect_snowflake.py`
3. generate create table statements for staging `generate_create_stg_tables.py`
4. run the create table script `output/create_staging_tables.py`
5. generate the load table statements for staging
6. Fill the staging tables with random data
7. Create the source-to-target-mapping (hub & sat metadata)
8. Create hubs and sats
9. Create link metadata 
9. Create links
10. Create load hubs statements
11. Create load links statements
12. Create load sats statements
13. Load hubs
14. Load links
15. Load sats



Remark: Presently, everything goes into the schema configured in the config.ini


