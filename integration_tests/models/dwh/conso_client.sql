{{-
    config(
        materialized='incremental_stream',
        unique_key=['ID']
    )
-}}
with client_with_dup as (
    SELECT row_number() OVER (PARTITION BY ID ORDER BY CREATED_AT DESC) as NB_DUP,
    ID, FIRST_NAME, LAST_NAME, BIRTHDATE, CREATED_AT
    FROM {{incr_stream.stream_ref('add_clients')}}
), clients_without_dup as (
    SELECT ID, FIRST_NAME, LAST_NAME, BIRTHDATE, CREATED_AT FROM client_with_dup WHERE NB_DUP = 1 ORDER BY CREATED_AT
)
SELECT * FROM clients_without_dup