CREATE TABLE `{dest_table}`
PARTITION BY DATE(loaded_at)
CLUSTER BY company_id, park_id
AS
-- CTE 1: Parse the JSON and unnest the parks array
-- UNNEST converts the JSON string array into individual rows
WITH parks_unnested AS (
  SELECT
    id AS company_id,
    name AS company_name,
    CAST(year AS INT64) AS year,
    CAST(month AS INT64) AS month,
    CAST(day AS INT64) AS day,
    CAST(hour AS INT64) AS hour,
    CAST(minute AS INT64) AS minute,
    park  -- Individual park object from unnested array
  FROM `{source_table}`,
  UNNEST(JSON_EXTRACT_ARRAY(parks)) AS park
),

-- CTE 2: Extract individual fields from each park JSON object
parks_flattened AS (
  SELECT
    company_id,
    company_name,
    year,
    month,
    day,
    hour,
    minute,
    CAST(JSON_EXTRACT_SCALAR(park, '$.id') AS INT64) AS park_id,
    JSON_EXTRACT_SCALAR(park, '$.name') AS park_name,
    JSON_EXTRACT_SCALAR(park, '$.continent') AS continent,
    JSON_EXTRACT_SCALAR(park, '$.country') AS country,
    CAST(JSON_EXTRACT_SCALAR(park, '$.latitude') AS FLOAT64) AS latitude,
    CAST(JSON_EXTRACT_SCALAR(park, '$.longitude') AS FLOAT64) AS longitude,
    JSON_EXTRACT_SCALAR(park, '$.timezone') AS timezone
  FROM parks_unnested
),

-- CTE 3: Create a proper timestamp and add data quality checks
parks_enriched AS (
  SELECT
    company_id,
    company_name,
    park_id,
    park_name,
    continent,
    country,
    latitude,
    longitude,
    timezone,
    -- Combine date and time fields into a proper timestamp
    TIMESTAMP(
      DATETIME(
        year, month, day, hour, minute, 0
      )
    ) AS loaded_at,
    -- Add data quality indicators
    CASE 
      WHEN latitude IS NULL OR longitude IS NULL THEN FALSE
      ELSE TRUE 
    END AS has_coordinates,
    CASE
      WHEN timezone IS NULL THEN FALSE
      ELSE TRUE
    END AS has_timezone
  FROM parks_flattened
)

-- Final silver layer table
SELECT
  company_id,
  company_name,
  park_id,
  park_name,
  continent,
  country,
  latitude,
  longitude,
  timezone,
  loaded_at,
  has_coordinates,
  has_timezone,
  CURRENT_TIMESTAMP() AS processed_at
FROM parks_enriched;