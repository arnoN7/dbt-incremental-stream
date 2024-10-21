{{-
    config(
        materialized='incremental_stream',
        unique_key=['ID']
    )
-}}
with client_with_dup as (
    SELECT row_number() OVER (PARTITION BY ID ORDER BY LOADED_AT DESC) as NB_DUP,
    ID, PRENOM, NOM, DATE_NAISSANCE, LOADED_AT {{ incr_stream.get_stream_metadata_columns() }}
    FROM {{incr_stream.stream_ref('v_conso_client')}}
), clients_without_dup as (
    SELECT ID, PRENOM, NOM, DATE_NAISSANCE, LOADED_AT {{ incr_stream.get_stream_metadata_columns() }} FROM client_with_dup WHERE NB_DUP = 1 ORDER BY LOADED_AT
)
SELECT * FROM clients_without_dup