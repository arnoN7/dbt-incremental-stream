{{
    config(
        materialized='incremental',
        unique_key='id'
    )
}}

-- Create test data with quoted/case-sensitive column names
-- This tests the fix for proper column identifier quoting

{% if not is_incremental() %}
    -- Initial load with special column names
    SELECT
        0 as id,
        'John Doe' as "Client Name",  -- Column with space
        'JOHN.DOE@EMAIL.COM' as "EMAIL_ADDRESS",  -- Case-sensitive column
        CURRENT_TIMESTAMP() as "Created_At",  -- Mixed case column
        'Inactive' as status  -- Use longest status value first
    UNION ALL
    SELECT
        1 as id,
        'Jane Smith' as "Client Name",
        'JANE.SMITH@EMAIL.COM' as "EMAIL_ADDRESS",
        CURRENT_TIMESTAMP() as "Created_At",
        'Inactive' as status
{% else %}
    -- Incremental load - add new records or update existing ones
    SELECT
        2 as id,
        'Bob Jones' as "Client Name",
        'BOB.J@EMAIL.COM' as "EMAIL_ADDRESS",
        CURRENT_TIMESTAMP() as "Created_At",
        'Active' as status
    UNION ALL
    SELECT
        1 as id,  -- Update existing record
        'Jane S' as "Client Name",
        'JANE.S@EMAIL.COM' as "EMAIL_ADDRESS",
        CURRENT_TIMESTAMP() as "Created_At",
        'Inactive' as status
{% endif %}
