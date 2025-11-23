from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from pydantic_settings import BaseSettings
from tools.logger import get_logger
import os

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

    def get_sql_query(self, source_table, dest_table):
        """Reads SQL from file and formats it."""
        sql_path = "sql/transform_queue_times.sql"
        
        with open(sql_path, 'r') as f:
            query_template = f.read()
            
        # We inject the table names into the external SQL file
        return query_template.format(
            source_table=source_table,
            dest_table=dest_table
        )
    
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
        
        # 2. Get the clean SQL from file
        query = self.get_sql_query(source_table, dest_table)
        
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