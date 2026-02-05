{{-
    config(
        materialized='incremental_stream',
        unique_key=['id']
    )
-}}
-- This model tests proper column quoting with special cases:
-- 1. Column with space: "Client Name"
-- 2. Case-sensitive columns: "EMAIL_ADDRESS", "Created_At"
-- 3. Regular columns: id, status

with client_dedup as (
    SELECT
        row_number() OVER (PARTITION BY id ORDER BY "Created_At" DESC) as rnk,
        id,
        "Client Name",
        "EMAIL_ADDRESS",
        "Created_At",
        status
        {{ incr_stream.get_stream_metadata_columns() }}
    FROM {{incr_stream.stream_ref('add_clients_quoted')}}
), clients_final as (
    SELECT
        id,
        "Client Name",
        "EMAIL_ADDRESS",
        "Created_At",
        status
        {{ incr_stream.get_stream_metadata_columns() }}
    FROM client_dedup
    WHERE rnk = 1
)
SELECT * FROM clients_final
