{{-
    config(
        materialized='incremental_stream'
    )
-}}
SELECT * FROM {{incr_stream.stream_ref('add_clients')}}