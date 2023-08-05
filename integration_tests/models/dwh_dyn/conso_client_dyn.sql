{{
    config(
        materialized='dynamic_table',
        target_lag='120 seconds',
        snowflake_warehouse='DYN_WAREHOUSE'
    )
}}
with client_with_dup as (
    SELECT row_number() OVER (PARTITION BY ID ORDER BY LOADED_AT DESC) as NB_DUP,
    ID, FIRST_NAME, LAST_NAME, BIRTHDATE, LOADED_AT
    FROM {{ref('add_clients')}}
), clients_without_dup as (
    SELECT ID, FIRST_NAME, LAST_NAME, BIRTHDATE, LOADED_AT FROM client_with_dup WHERE NB_DUP = 1 ORDER BY LOADED_AT
)
SELECT * FROM clients_without_dup