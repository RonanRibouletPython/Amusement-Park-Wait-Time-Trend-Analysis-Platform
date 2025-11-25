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
    
    QUEUE_TIMES_RAW_TABLE: str = "queue_times"
    QUEUE_TIMES_SILVER_TABLE: str = "queue_times_cleaned"
    
    PARKS_METADATA_RAW_TABLE: str = "parks_metadata"
    PARKS_METADATA_SILVER_TABLE: str = "parks_metadata_cleaned"
    
    LOCATION: str = "EU"

# Load settings
settings = Settings()

class SilverLayer():
    def __init__(self):
        self.client = bigquery.Client(project=settings.GCP_PROJECT_ID)
        
    def setup_silver_dataset(self, project_id: str, dataset_id: str, location: str) -> None:
        """Ensures the destination dataset exists."""
        dataset_id = f"{project_id}.{dataset_id}"
        try:
            self.client.get_dataset(dataset_id)
            logger.info(f"Dataset {dataset_id} already exists")
        except NotFound:
            logger.info(f"Creating dataset {dataset_id}...")
            dataset = bigquery.Dataset(dataset_id)
            dataset.location = location
            self.client.create_dataset(dataset)
            logger.info(f"Dataset created successfully")

    def get_sql_query(self, sql_path: str, source_table: str, dest_table: str) -> str:
        """Reads SQL from file and formats it."""
        
        with open(sql_path, 'r') as f:
            query_template = f.read()
            
        # We inject the table names into the external SQL file
        return query_template.format(
            source_table=source_table,
            dest_table=dest_table
        )
    
    def drop_table(self, project_id: str, dataset_id: str, table_id: str):
        """Drops a table if it exists to allow fresh creation."""
        table = f"{project_id}.{dataset_id}.{table_id}"
        try:
            self.client.delete_table(table)
            logger.info(f"Dropped existing table {table} to ensure a fresh schema/partitioning.")
        except NotFound:
            logger.info(f"Table {table} does not exist... proceeding.")
    
    def transform_and_load(self,
                           source_project_id: str,
                           source_dataset: str,
                           source_table: str,
                           dest_project_id: str,
                           dest_dataset: str,
                           dest_table: str,
                           sql_path: str
                        ):
        logger.info(f"Starting Silver Layer transformation using Native SQL...")
        
        # 1. Clean up old table first
        self.drop_table(project_id=dest_project_id, dataset_id=dest_dataset, table_id=dest_table)

        source_table = f"{source_project_id}.{source_dataset}.{source_table}"
        dest_table = f"{dest_project_id}.{dest_dataset}.{dest_table}"
        
        logger.info(f"Source Table: {source_table}")
        logger.info(f"Destination Table: {dest_table}")
        
        # 2. Get the clean SQL from file
        query = self.get_sql_query(sql_path=sql_path, source_table=source_table, dest_table=dest_table)
        
        # 3. Execute the query
        logger.debug(f"Executing SQL:\n{query}")
        
        try:
            job = self.client.query(query)
            job.result()  # Wait for the job to complete
            logger.info(f"Silver Layer transformation complete. Data loaded into {dest_table}")
        except Exception as e:
            logger.error(f"SQL execution failed: {e}")
            raise

    def run_pipeline(self,
                     project_id: str,
                     source_dataset: str,
                     source_table: str,
                     dest_dataset: str,
                     dest_table: str,
                     location: str, 
                     sql_path: str):
        self.setup_silver_dataset(project_id=project_id, dataset_id=dest_dataset, location=location)
        self.transform_and_load(source_project_id=project_id,
                                source_dataset=source_dataset,
                                source_table=source_table,
                                dest_project_id=project_id,
                                dest_dataset=dest_dataset,
                                dest_table=dest_table,
                                sql_path=sql_path,
                                )

if __name__ == "__main__":
    queue_times_sql_path = "sql/transform_queue_times.sql"
    parks_metadata_sql_path = "sql/transform_parks_metadata.sql"
    job = SilverLayer()
    # Run the job for Queue Times
    job.run_pipeline(project_id=settings.GCP_PROJECT_ID,
                     source_dataset=settings.RAW_DATASET,
                     source_table=settings.QUEUE_TIMES_RAW_TABLE,
                     dest_dataset=settings.DERIVED_DATASET,
                     dest_table=settings.QUEUE_TIMES_SILVER_TABLE,
                     location=settings.LOCATION,
                     sql_path=queue_times_sql_path)
    # Run the job for Parks Metadata
    job.run_pipeline(project_id=settings.GCP_PROJECT_ID,
                     source_dataset=settings.RAW_DATASET,
                     source_table=settings.PARKS_METADATA_RAW_TABLE,
                     dest_dataset=settings.DERIVED_DATASET,
                     dest_table=settings.PARKS_METADATA_SILVER_TABLE,
                     location=settings.LOCATION,
                     sql_path=parks_metadata_sql_path)