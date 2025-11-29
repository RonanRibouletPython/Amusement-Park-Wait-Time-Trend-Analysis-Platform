from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from tools.logger import get_logger
from data_ingestion import Settings
import os

logger = get_logger(__name__)


# Load settings
settings = Settings()

class DataTransformation():
    def __init__(self):
        self.client = bigquery.Client(project=settings.GCP_PROJECT_ID)
        
    def setup_dataset(self, project_id: str, dataset_id: str, location: str) -> None:
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

    def get_sql(self, path: str, source_table: str, dest_table: str) -> str:
        with open(path, 'r') as f:
            query = f.read()
        return query.format(source_table=source_table, dest_table=dest_table)
    
    def run_query(self, sql_path: str, source_table_name: str, dest_table_name: str):
        # Construct full table IDs
        source_full = f"{settings.GCP_PROJECT_ID}.{settings.RAW_DATASET}.{source_table_name}"
        dest_full = f"{settings.GCP_PROJECT_ID}.{settings.DERIVED_DATASET}.{dest_table_name}"
        
        # Prepare SQL (Injecting source and destination table names)
        query = self.get_sql(sql_path, source_full, dest_full)
        
        logger.info(f"Running transformation: {source_table_name} -> {dest_table_name}")
        try:
            job = self.client.query(query)
            job.result() # Wait for completion
            destination_table = self.client.get_table(dest_full)
            logger.info(f"Success: Loaded {destination_table.num_rows} rows into {dest_table_name}")
        except Exception as e:
            logger.error(f"Transformation failed: {e}")
            raise

    def process_all(self):
        """Main entry point to run all transformations"""
        logger.info("Starting Silver Layer Transformation...")
        
        # Ensure target dataset exists
        self.setup_dataset(project_id=settings.GCP_PROJECT_ID, dataset_id=settings.DERIVED_DATASET, location=settings.BUCKET_LOCATION)
        
        # 1. Transform Queue Times
        logger.info("Transforming Queue Times...")
        self.run_query(
            sql_path="sql/transform_queue_times.sql",
            source_table_name=settings.QUEUE_TIMES_RAW_TABLE,
            dest_table_name=settings.QUEUE_TIMES_SILVER_TABLE
        )
        
        # 2. Transform Parks Metadata
        logger.info("Transforming Parks Metadata...")
        self.run_query(
            sql_path="sql/transform_parks_metadata.sql",
            source_table_name=settings.PARKS_METADATA_RAW_TABLE,
            dest_table_name=settings.PARKS_METADATA_SILVER_TABLE
        )