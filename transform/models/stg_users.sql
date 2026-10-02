WITH source AS (
    SELECT * FROM {{ source('raw_data', 'users_raw') }}
)

SELECT
    id AS user_id,
    CONCAT(first_name, ' ', last_name) AS full_name,
    -- Cast age back to an integer for analytics, defaulting to NULL if empty
    CAST(NULLIF(age, '') AS INTEGER) AS age,
    -- Handle the additive drift by coalescing nulls into a default tier
    COALESCE(loyalty_tier, 'Standard') AS loyalty_tier,
    created_at
FROM source