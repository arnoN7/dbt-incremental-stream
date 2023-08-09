{{-
    config(
        materialized='incremental'
    )
-}}
SELECT * FROM {{ref('add_clients')}}
{% if is_incremental() %}
      WHERE LOADED_AT >= coalesce((select max(LOADED_AT) from {{ this }}), '1900-01-01')
{% endif %}