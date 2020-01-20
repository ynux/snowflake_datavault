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

is the Production Schema of Microsoft's [AdventureWorks sample data warehouse](https://github.com/microsoft/sql-server-samples/tree/master/samples/databases/adventure-works)

### What to use this for

Just practice, not real production - you should buy a tool.

### Metadata Design

The metadata is designed for [the TEAM metadata of Roelant Vos](https://github.com/RoelantVos/TEAM)

### Notes on the Code

written for python 3.7
install the requirements
for tests, there should be a sqlite version

### How to use this

1. Prepare input data
2. Establish connection to Snowflake
3. generate create table statements for stg
4. combine into create table script

