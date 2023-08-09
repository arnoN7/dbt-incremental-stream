{{-
    config(
        materialized='incremental_stream',
        unique_key=['ID']
    )
-}}
with client_with_dup as (
    SELECT row_number() OVER (PARTITION BY ID ORDER BY LOADED_AT DESC) as NB_DUP,
    ID, FIRST_NAME, LAST_NAME, BIRTHDATE, LOADED_AT {{ incr_stream.get_stream_metadata_columns() }}
    FROM {{incr_stream.stream_source('SOURCE_CRM', 'SOURCE_CLIENTS')}}
), clients_without_dup as (
    SELECT ID, FIRST_NAME, LAST_NAME, BIRTHDATE, LOADED_AT {{ incr_stream.get_stream_metadata_columns() }} FROM client_with_dup WHERE NB_DUP = 1 ORDER BY LOADED_AT
)
SELECT * FROM clients_without_dup