{% docs conso_client %}
## Client consolidation
**Table consolidating clients from add_clients**. Contains **the last update of clients only** (based on ID and LOADED_AT columns)
1. Collect lastest data ingested in `add_clients`
2. De-duplicate it based on `ID` with most recent `LOADED_AT` from `add_clients` stream or table (in --full-refresh mode)
3. `MERGE` data in `CONSO_CLIENTS` table with `ID` as unique key 
> **Note**
> Use on `incremental_stream` materialisation collecting data from [Streams](https://docs.snowflake.com/en/user-guide/streams-intro) 
{% enddocs %}