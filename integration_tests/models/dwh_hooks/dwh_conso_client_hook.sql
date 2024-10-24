{{-
    config(
        materialized='incremental_stream',
        unique_key=['ID'],
        pre_hook='CREATE OR REPLACE TABLE {{this.database}}.{{this.schema}}.pre_hook_test as select 1 as test;',
        post_hook='CREATE OR REPLACE TABLE {{this.database}}.{{this.schema}}.post_hook_test as select 1 as test;'
    )
-}}
with client_with_dup as (
    SELECT row_number() OVER (PARTITION BY ID ORDER BY LOADED_AT DESC) as NB_DUP,
    ID, FIRST_NAME, LAST_NAME, BIRTHDATE, LOADED_AT {{ incr_stream.get_stream_metadata_columns() }}
    FROM {{incr_stream.stream_ref('add_clients')}}
), clients_without_dup as (
    SELECT ID, FIRST_NAME, LAST_NAME, BIRTHDATE, LOADED_AT {{ incr_stream.get_stream_metadata_columns() }} FROM client_with_dup WHERE NB_DUP = 1 ORDER BY LOADED_AT
)
SELECT * FROM clients_without_dup