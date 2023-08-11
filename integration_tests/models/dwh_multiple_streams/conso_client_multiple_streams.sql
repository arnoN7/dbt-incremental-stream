{{-
    config(
        materialized='incremental_stream',
        unique_key=['ID', 'SOURCE']
    )
-}}
with client_web as (
    SELECT ID, 'WEB' as SOURCE, FIRST_NAME, LAST_NAME, BIRTHDATE, LOADED_AT {{ incr_stream.get_stream_metadata_columns() }}
    FROM {{incr_stream.stream_ref('add_clients')}}
),
client_retail as (
    SELECT ID, 'RETAIL' as SOURCE, FIRST_NAME, LAST_NAME, BIRTHDATE, LOADED_AT {{ incr_stream.get_stream_metadata_columns() }}
    FROM {{incr_stream.stream_source('SOURCE_CRM', 'SOURCE_CLIENTS')}}
), client_web_2 as (
    SELECT ID, 'WEB2' as SOURCE, FIRST_NAME, LAST_NAME, BIRTHDATE, LOADED_AT {{ incr_stream.get_stream_metadata_columns() }}
    FROM {{incr_stream.stream_ref('add_clients_')}}
),
union_clients as (
    SELECT * from client_web
    UNION
    SELECT * FROM client_retail
    UNION
    SELECT * FROM client_web_2
),
client_with_dup as (
    SELECT row_number() OVER (PARTITION BY ID, SOURCE ORDER BY LOADED_AT DESC) as NB_DUP,
    ID, SOURCE, FIRST_NAME, LAST_NAME, BIRTHDATE, LOADED_AT {{ incr_stream.get_stream_metadata_columns() }}
    FROM union_clients
), clients_without_dup as (
    SELECT ID, SOURCE, FIRST_NAME, LAST_NAME, BIRTHDATE, LOADED_AT {{ incr_stream.get_stream_metadata_columns() }} FROM client_with_dup WHERE NB_DUP = 1 ORDER BY LOADED_AT
)
SELECT * FROM clients_without_dup