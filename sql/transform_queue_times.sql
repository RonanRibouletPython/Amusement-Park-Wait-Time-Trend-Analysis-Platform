
CREATE OR REPLACE TABLE `{dest_table}`
PARTITION BY DATE(timestamp)
AS
WITH base AS (
    SELECT
        -- Construct Timestamp from the Hive partition columns
        PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', 
            CONCAT(year, '-', month, '-', day, ' ', hour, ':', minute, ':00')
        ) as timestamp,
        CAST(park_id AS INT64) as park_id,
        -- Columns are already native JSON in BigQuery
        rides as rides_json,
        lands as lands_json
    FROM `{source_table}`
),

-- Pipeline A: Extract rides that are at the root level
root_rides AS (
    SELECT
        timestamp,
        park_id,
        NULL as land_id,
        "General" as land_name,
        -- Extract scalars safely
        CAST(JSON_VALUE(ride, '$.id') AS INT64) as ride_id,
        JSON_VALUE(ride, '$.name') as ride_name,
        CAST(JSON_VALUE(ride, '$.is_open') AS BOOL) as is_open,
        CAST(JSON_VALUE(ride, '$.wait_time') AS INT64) as wait_time,
        CAST(JSON_VALUE(ride, '$.last_updated') AS TIMESTAMP) as last_updated
    FROM base,
    -- Unnest directly using JSON_QUERY_ARRAY
    UNNEST(JSON_QUERY_ARRAY(rides_json)) as ride
),

-- Pipeline B: Extract rides nested inside lands
nested_rides AS (
    SELECT
        timestamp,
        park_id,
        CAST(JSON_VALUE(land, '$.id') AS INT64) as land_id,
        JSON_VALUE(land, '$.name') as land_name,
        CAST(JSON_VALUE(ride, '$.id') AS INT64) as ride_id,
        JSON_VALUE(ride, '$.name') as ride_name,
        CAST(JSON_VALUE(ride, '$.is_open') AS BOOL) as is_open,
        CAST(JSON_VALUE(ride, '$.wait_time') AS INT64) as wait_time,
        CAST(JSON_VALUE(ride, '$.last_updated') AS TIMESTAMP) as last_updated
    FROM base,
    -- First Unnest: Get the lands
    UNNEST(JSON_QUERY_ARRAY(lands_json)) as land,
    -- Second Unnest: Get the rides inside the current land
    UNNEST(JSON_QUERY_ARRAY(land, '$.rides')) as ride
)

-- Union them together
SELECT * FROM root_rides
UNION ALL
SELECT * FROM nested_rides