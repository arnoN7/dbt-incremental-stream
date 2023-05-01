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
* Create `{{this}}` table `IF NOT EXISTS`
* Create required stream `IF NOT EXISTS`
* `CREATE OR REPLACE` a view to perform the transformation developped in the model 
*  If stream is empty **materialization stops** and prevent `RESUME` of Snowflake Warehouse   
* `MERGE` the view with `{{this}}` based on `unique_key` provided


