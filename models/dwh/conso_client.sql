{{-
    config(
        materialized='incremental_stream',
        unique_key=['ID'],
        schema='DWH'
    )
-}}
with union_client as (
select * FROM {{incr_stream.stream_ref('add_clients')}} 
UNION 
select * FROM {{incr_stream.stream_ref('add_clients_web')}}), 
client_with_dup as (
    SELECT row_number() OVER (PARTITION BY ID ORDER BY LOADED_AT DESC) as NB_DUP,
    ID, FIRST_NAME, LAST_NAME, BIRTHDATE, LOADED_AT
    FROM union_client
), clients_without_dup as (
    SELECT ID, FIRST_NAME, LAST_NAME, BIRTHDATE, LOADED_AT FROM client_with_dup WHERE NB_DUP = 1 ORDER BY LOADED_AT
)
SELECT * FROM clients_without_dup