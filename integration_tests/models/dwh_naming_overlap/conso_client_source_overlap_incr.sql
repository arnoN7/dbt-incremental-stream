{{-
    config(
        materialized='incremental',
        unique_key=['ID']
    )
-}}
with client_with_dup as (
    SELECT row_number() OVER (PARTITION BY ID ORDER BY LOADED_AT DESC) as NB_DUP,
    ID, FIRST_NAME, LAST_NAME, BIRTHDATE, LOADED_AT
    FROM {{source('SOURCE_CRM', 'SOURCE_CLIENTS')}}
), clients_without_dup as (
    SELECT ID, FIRST_NAME, LAST_NAME, BIRTHDATE, LOADED_AT FROM client_with_dup 
    WHERE NB_DUP = 1 
    {% if is_incremental() %}
      and LOADED_AT >= coalesce((select max(LOADED_AT) from {{ this }}), '1900-01-01')
    {% endif %}
    ORDER BY LOADED_AT
)
SELECT * FROM clients_without_dup