# dbt-incremental-stream
**This dbt package is for Snowflake ❄️ only.**

**It is reproducing dbt incremental materialization** leveraging on Snowflake [streams](https://docs.snowflake.com/en/user-guide/streams-intro) to :
* **Improve model performance 💨**. [Stream](https://docs.snowflake.com/en/user-guide/streams-intro) accelerate source table scan focusing new data only 
* **Optimise FinOps 💰**. Snowflake warehouse will be started only if [stream](https://docs.snowflake.com/en/user-guide/streams-intro) is not empty. 
* Simplify code. `is_incremental()` is generally not required as [stream](https://docs.snowflake.com/en/user-guide/streams-intro) natively collect "new" rows

![](readme/stream.gif)

## Installation Instruction
New to dbt packages? Read more about them [here](https://docs.getdbt.com/docs/building-a-dbt-project/package-management/).
1. Include this package in your `packages.yml`. An example can be found [here](/integration_tests/packages.yml) in the [working example project](#working-example) 
2. Run `dbt deps` 

# Usage
Use `incremental_stream` materialisation like [dbt incremental model](https://docs.getdbt.com/docs/build/incremental-models) :
* Replace `ref` by `stream_ref` macro to add a [stream](https://docs.snowflake.com/en/user-guide/streams-intro) on a dbt model
* Replace `source`by `stream_source` to add a [stream](https://docs.snowflake.com/en/user-guide/streams-intro) on a source
* `incr_stream.get_stream_metadata_columns()` must be included to retreive METADATA columns of each `STREAMS`. Hence the materialization can deal accordingly with the changes on inputs tables (`INSERT`, `UPDATE`, `DELETE`) but Metadata columns (`METADATA$ACTION`, `METADATA$ISUPDATE`, `METADATA$ROW_ID`) will not be included in the model
* Like in [dbt incremental model](https://docs.getdbt.com/docs/build/incremental-models#defining-a-unique-key-optional) `unique_key` optional parameter is supported. DBT will perform a `MERGE` with a unique_key an `INSERT` instead.
* Like in [dbt incremental model](https://docs.getdbt.com/docs/build/incremental-models#how-do-i-rebuild-an-incremental-model) `--full-refresh` rebuild the target model based on the source tables

> [!NOTE]
> `{{ incr_stream.get_stream_metadata_columns() }}` should not be included if `*` is used to get all the columns from the source like in the example below :  
> ```
>{{-
>    config(
>        materialized='incremental_stream',
>        unique_key=['ID']
>    )
>    select 
>        *
>    from incr_stream.stream_source('src', 'src_table')
>-}}
>```

## Basic examples using one table as input
### Example using a unique key and a ref
A test model [conso_client.sql](/integration_tests/models/dwh_ref/conso_client.sql) following this pattern can be found in the [integration_tests](/integration_tests/) sub DBT project included in the package.
```
{{-
    config(
        materialized='incremental_stream',
        unique_key=['column1']
    )
    select 
        column1 as c_1,
        ...,
        columnN as c_n
        {{ incr_stream.get_stream_metadata_columns() }}
    from incr_stream.stream_ref('input_model')
-}}
```
### Example using a unique key and a source
A test model [conso_client_source.sql](/integration_tests/models/dwh_source/conso_client_source.sql) following this pattern can be found in the [integration_tests](/integration_tests/) sub DBT project included in the package.

Exemple with source
```
{{-
    config(
        materialized='incremental_stream',
        unique_key=['column1']
    )
    select 
        column1 as c_1,

        ...,
        columnN as c_n
        {{ incr_stream.get_stream_metadata_columns() }}
    from incr_stream.stream_source('src', 'src_table')
-}}
```

### Example not using a unique key
Can be found in this test model: [conso_client_insert](/integration_tests/models/dwh_insert/conso_client_insert.sql)

### Example performing UNION on multiple tables
A test model [conso_client_multiple_streams.sql](/integration_tests/models/dwh_multiple_streams/conso_client_multiple_streams.sql) implementing this pattern can be found in the [integration_tests](/integration_tests/) sub DBT project included in the package.

![lineage_streams](/readme/multiple_streams.png)


# Macros
`stream_ref` and `stream_source` macro include created [streams](https://docs.snowflake.com/en/user-guide/streams-intro) in the compiled SQL code of the model. 
* [Stream](https://docs.snowflake.com/en/user-guide/streams-intro) created in `{{this}}.database` and `{{this}}.schema`
* [Stream](https://docs.snowflake.com/en/user-guide/streams-intro) name follows this naming convention : `S_{{this.name}}_{{source_table_name}}`
* `get_stream_metadata_columns` is used to easily retreive metadata columns from the stream (`METADATA$ACTION`, `METADATA$ISUPDATE`, `METADATA$ROW_ID`)


> [!NOTE]
> When using `--full-refresh` flag, macros return `ref` and `source` (not streams) to perform a complete rebuild. 

| macro | description |
|-------|-------------|
| `stream_ref` ([source](macros/stream_source.sql)) | Replace `ref` by the stream name (if not `--full-refresh` flag) |
| `stream_source` ([source](macros/stream_source.sql)) | Replace `source` by the stream name (if not `--full-refresh` flag) |


# Materialization
Incremental materialization will perform the following tasks : 
1. Create `{{this}}` table `IF NOT EXISTS`
2. Create required [stream](https://docs.snowflake.com/en/user-guide/streams-intro) `IF NOT EXISTS`
3. `CREATE OR REPLACE` a view to perform the transformation developped in the model 
4.  If [stream](https://docs.snowflake.com/en/user-guide/streams-intro) is empty **materialization stops** and prevent Snowflake Warehouse `RESUME`  
5. `MERGE` the view in `{{this}}` table based on `unique_key` provided


# Working Example
[integration_test](/integration_tests/) contains a working example with **models and a test**:
* To validate `incremental_stream` and `incremental` materialization produce the same output
* To do performance test 

| model | description |
|-------|-------------|
| [add_clients](#add_clients-model) ([source](/integration_tests/models/stg/add_clients.py)) and [add_clients_](#add_clients-model) ([source](/integration_tests/models/stg/add_clients_.py))| Python 🐍 incremental models adding new random clients. To simulate a stream source like a Kafka topic |
| [conso_client](#conso_client-model) ([source](/integration_tests/models/dwh/conso_client.sql)) | `incremental_stream` model de-duplicating clients on `ID` |
| [conso_client_incr](#conso_client-model) ([source](/integration_tests/models/dwh/conso_client_incr.sql)) | standard dbt `incremental` model to compare result and performance | model to compare result and performance with `conso_client_insert` |

And more to explore in sub DBT Project [integration_tests](/integration_tests/)

![lineage](/readme/lineage.png)

```
# 1. Add 30 new random clients to ADD_CLIENTS table 
# 2. Merge it in CONSO_CLIENT and CONSO_CLIENT_INCR
# 3. Test CONSO_CLIENT and CONSO_CLIENT_INCR equals
cd ./integration_test
dbt build
```

## add_clients model
Python 🐍 incremental model :
1. Creating a `ADD_CLIENTS` table if not exists in a `STG` schema 
2. And adding **random clients** using [Faker](https://faker.readthedocs.io/en/master/) library.

**ADD_CLIENTS Table**
| ID | FIRST_NAME  | LAST_NAME | BIRTHDATE  |    LOADED_AT     |
|----|-------------|----------:|-----------:|------------------:|
| 1  |     Amy     |  Patton   | 1985-10-08 |2023-04-28 15:01:10|
| 2  |     Eric    |  Ferguson | 1973-09-15 |2023-04-28 15:01:10|
| 3  |     Amy     |  Weston   | 1985-10-08 |2023-04-28 15:01:10|

> [!NOTE]
>`nb_clients` [variable](https://docs.getdbt.com/docs/using-variables) defines the number of random clients to add (default 30)

**Sample commands** 
```
# Add 30 random clients in add_clients model (create table if needed)
dbt run --select add_clients

# Add 100 random clients in add_clients model (create table if needed)
dbt run --select add_clients --vars '{nb_clients: 100}'

# Rebuild ADD_CLIENTS table with 30 new random clients
dbt run --select add_clients --full-refresh
```

## conso_client model
A sample model leveraging on `incremental_stream` custom materialization
1. Collecting lastest data ingested in [add_clients](#add_clients-model)
2. De-duplicating it based on `ID` with most recent `LOADED_AT` from [add_clients](#add_clients-model) stream or table
3. `MERGE` data in `CONSO_CLIENTS` table with `ID` as unique key 

**Sample commands** 
```
# Merge last clients inserted in add_clients model
dbt run --select conso_clients

# Drop CONSO_CLIENTS table and rebuild it based on ADD_CLIENTS table
dbt run --select conso_clients --full-refresh
```

## Limitations

Current known limitations : 
* When a `stream_ref` or a `stream_source` table is dropped and recreated [stream](https://docs.snowflake.com/en/user-guide/streams-intro) must be dropped. If not you will observe the following error
```
 091901 (01000): Base table 'DB_NAME.SCH_NAME.XXX_TABLE' dropped, cannot read from stream 'S_YYY_XXX_TABLE' in line 1 position 21
```
* `on_schema_change` parameter might work in certain conditions but is not currently supported  

## Credits
Thanks to [jeremiahhansen ❄️](https://github.com/jeremiahhansen) for the inspiring [implementation](https://github.com/jeremiahhansen/snowflake-helper-dbt) (done in 2020) of streams & tasks on Snowflake
