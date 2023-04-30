# dbt-incremental-stream
**This dbt package is for Snowflake only.**

**It is reproducing dbt incremental materialization** leveraging on Snowflake [Streams](https://docs.snowflake.com/en/user-guide/streams-intro) to :
* Improve model performance. Stream accelerate source table scan focusing new data only 
* Simplify model code. `is_incremental()` is generally not required as Stream natively collect "new" rows

## Installation Instruction
New to dbt packages? Read more about them [here](https://docs.getdbt.com/docs/building-a-dbt-project/package-management/).
1. Include this package in your `packages.yml` 
2. Run `dbt deps` 

## Usage
![readme/stream.mp4](https://github.com/arnoN7/dbt-incremental-stream/blob/df47e7eca2a0711ced0a52f3fcb59bdd7ae0cdcf/readme/stream.mp4)
