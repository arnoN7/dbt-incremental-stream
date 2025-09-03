{{-
    config(
        materialized='view'
    )
-}}
SELECT ID, FIRST_NAME AS PRENOM, LAST_NAME AS NOM, BIRTHDATE AS DATE_NAISSANCE, LOADED_AT FROM {{ source('SOURCE_CRM', 'SOURCE_CLIENTS')}}