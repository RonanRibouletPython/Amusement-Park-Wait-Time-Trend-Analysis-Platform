from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from pydantic_settings import BaseSettings
from tools.logger import get_logger

logger = get_logger(__name__)

class Settings(BaseSettings):
    GCP_PROJECT_ID: str = "amusement-park-wait-time"
    RAW_DATASET: str = "amusement_park_raw" 
    DERIVED_DATASET: str = "amusement_park_derived" 
    
    RAW_TABLE: str = "queue_times"
    SILVER_TABLE: str = "queue_times_cleaned"
    
    LOCATION: str = "EU"

settings = Settings()

class SilverLayer():
    def __init__(self):
        self.client = bigquery.Client(project=settings.GCP_PROJECT_ID)
        
    def setup_silver_dataset(self):
        """Ensures the destination dataset exists."""
        dataset_id = f"{settings.GCP_PROJECT_ID}.{settings.DERIVED_DATASET}"
        try:
            self.client.get_dataset(dataset_id)
            logger.info(f"Dataset {settings.DERIVED_DATASET} already exists")
        except NotFound:
            logger.info(f"Creating dataset {settings.DERIVED_DATASET}...")
            dataset = bigquery.Dataset(dataset_id)
            dataset.location = settings.LOCATION
            self.client.create_dataset(dataset)
            logger.info(f"Dataset created successfully")

    def drop_silver_table(self):
        """Drops the destination table if it exists to allow fresh creation."""
        dest_table = f"{settings.GCP_PROJECT_ID}.{settings.DERIVED_DATASET}.{settings.SILVER_TABLE}"
        try:
            self.client.delete_table(dest_table)
            logger.info(f"Dropped existing table {dest_table} to ensure a fresh schema/partitioning.")
        except NotFound:
            logger.info(f"Table {dest_table} does not exist; proceeding.")
    
    def transform_and_load(self):
        logger.info(f"Starting Silver Layer transformation using Native SQL...")
        
        # 1. Clean up old table first
        self.drop_silver_table()

        source_table = f"{settings.GCP_PROJECT_ID}.{settings.RAW_DATASET}.{settings.RAW_TABLE}"
        dest_table = f"{settings.GCP_PROJECT_ID}.{settings.DERIVED_DATASET}.{settings.SILVER_TABLE}"
        
        # 2. Define the transformation query
        # We use CREATE TABLE (not CREATE OR REPLACE, since we just dropped it)
        query = f"""
        CREATE TABLE `{dest_table}`
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
        """
        
        # 3. Execute the query
        logger.debug(f"Executing SQL:\n{query}")
        
        try:
            job = self.client.query(query)
            job.result()  # Wait for the job to complete
            logger.info(f"Silver Layer transformation complete. Data loaded into {dest_table}")
        except Exception as e:
            logger.error(f"SQL execution failed: {e}")
            raise

    def run_pipeline(self):
        self.setup_silver_dataset()
        self.transform_and_load()

if __name__ == "__main__":
    job = SilverLayer()
    job.run_pipeline()