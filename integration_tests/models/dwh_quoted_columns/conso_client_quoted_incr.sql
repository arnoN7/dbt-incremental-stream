{{-
    config(
        materialized='incremental',
        unique_key=['id']
    )
-}}
-- Standard incremental model for comparison

with client_dedup as (
    SELECT
        row_number() OVER (PARTITION BY id ORDER BY "Created_At" DESC) as rnk,
        id,
        "Client Name",
        "EMAIL_ADDRESS",
        "Created_At",
        status
    FROM {{ref('add_clients_quoted')}}
    {% if is_incremental() %}
    WHERE "Created_At" > (SELECT MAX("Created_At") FROM {{ this }})
    {% endif %}
), clients_final as (
    SELECT
        id,
        "Client Name",
        "EMAIL_ADDRESS",
        "Created_At",
        status
    FROM client_dedup
    WHERE rnk = 1
)
SELECT * FROM clients_final
