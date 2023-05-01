# dbt-incremental-stream
**This dbt package is for Snowflake ❄️ only.**

**It is reproducing dbt incremental materialization** leveraging on Snowflake [Streams](https://docs.snowflake.com/en/user-guide/streams-intro) to :
* **Improve model performance 💨**. Stream accelerate source table scan focusing new data only 
* **Optimise FinOps 💰**. Snowflake warehouse will be started only is stream is not empty. 
* Simplify code. `is_incremental()` is generally not required as Stream natively collect "new" rows

![](readme/stream.gif)

## Installation Instruction
New to dbt packages? Read more about them [here](https://docs.getdbt.com/docs/building-a-dbt-project/package-management/).
1. Include this package in your `packages.yml` 
2. Run `dbt deps` 

## Usage
Use `incremental_stream` materialisation like dbt incremental model and replace 
* `ref` by `stream_ref` macro to reference input model
* `source`by `stream_source` to reference an input source

Exemple with ref
```
{{-
    config(
        materialized='incremental_stream',
        unique_key=['ID']
    )
    select ...
    from incr_stream.stream_ref('input_model')
-}}
```

Exemple with source
```
{{-
    config(
        materialized='incremental_stream',
        unique_key=['ID']
    )
    select ...
    from incr_stream.stream_source('src', 'src_table')
-}}
```

# Macros
`stream_ref` and `stream_source` macro reference created streams in the SQL code of the model. 
* Stream created in `{{this}}.database` and `{{this.schema}}`
* Stream name follows the following name convention : `S_{{This.name}}_{{source_table_name}}`

> **Note**
> When using `--full-refresh` flag, macros return `ref` and `source` (not streams) to perform a complete rebuild. 

| macro | description |
|-------|-------------|
| `stream_ref` ([source](macros/stream_source.sql)) | Replace `ref` by the stream name (if not `--full-refresh` flag) |
| `stream_source` ([source](macros/stream_source.sql)) | Replace `source` by the stream name (if not `--full-refresh` flag) |


# Materialization
Incremental materialization will perform the following tasks : 
1. Create `{{this}}` table `IF NOT EXISTS`
2. Create required stream `IF NOT EXISTS`
3. `CREATE OR REPLACE` a view to perform the transformation developped in the model 
4.  If stream is empty **materialization stops** and prevent Snowflake Warehouse `RESUME`  
5. `MERGE` the view in `{{this}}` table based on `unique_key` provided


# Working Example
`/integration_test` contains a DBT project working example with two models.

| model | description |
|-------|-------------|
| [add_clients](#add_clients-model) ([source](/integration_tests/models/stg/add_clients.py)) | Python 🐍 incremental model adding new random clients. To simulate a stream source like a Kafka topic |
| [conso_clients](#conso_clients-model) ([source](/integration_tests/models/dwh/conso_client.sql)) | `incremental_stream` model de-duplicating clients on `ID` |

![lineage](/readme/lineage.png)
```
# Create ADD_CLIENT source or add new random clients and merge it in CONSO_CLIENTS
dbt run
```

## add_clients model
Python 🐍 incremental model :
1. Creating a `ADD_CLIENTS` table if not exists in a `STG` schema 
2. And adding random clients using [Faker](https://faker.readthedocs.io/en/master/) library.

## conso_clients model
A sample model leveraging on `incremental_stream` custom materialization
1. Collecting lastest data ingested in `add_clients`
2. De-duplicating it based on `ID` with most recent `CREATED_AT` from `add_clients` stream or table
3. `MERGE` data in `CONSO_CLIENTS` table with `ID` as unique key 

>**Note** 
>`nb_clients` [variable](https://docs.getdbt.com/docs/using-variables) define the default number of client added (default 30)

**Sample commands** 
```
# Merge last clients inserted in add_clients model
dbt run --select conso_clients

# Drop CONSO_CLIENTS table and rebuild it based on ADD_CLIENTS table
dbt run --select conso_clients --full-refresh
```
