from pydantic_settings import BaseSettings
from shared.bigquery_handler import BigQueryHandler
from google.cloud import bigquery # To create schema
from tools.logger import get_logger

logger = get_logger(__name__)

class InfrastructureSettings(BaseSettings):
    GCP_PROJECT_ID: str = "amusement-park-wait-time"
    BUCKET_NAME: str = "amusement-park-datalake-v1"
    BUCKET_LOCATION: str = "EU"
    
    # BigQuery Config
    DATASET_NAME: str = "amusement_park_raw"
    
    # Table 1: Metadata
    TABLE_PARKS: str = "parks_metadata"
    PATH_PARKS: str = "layer=bronze/source=parks_metadata/"
    
    # Table 2: Queue Times
    TABLE_QUEUES: str = "queue_times"
    PATH_QUEUES: str = "layer=bronze/source=queue_times/"

settings = InfrastructureSettings()

def infrastructure_setup():
    logger.info(f"Starting Bronze Layer Setup (SQL DDL Mode)...")
    
    bq = BigQueryHandler(
        project_id=settings.GCP_PROJECT_ID,
        location=settings.BUCKET_LOCATION
    )
    
    bq.create_dataset_if_not_exists(settings.DATASET_NAME)
    
    # ---------------------------------------------------------
    # 1. Parks Metadata
    # ---------------------------------------------------------
    logger.info("Configuring Parks Metadata table...")
    bq.create_external_table_via_sql(
        dataset_id=settings.DATASET_NAME,
        table_id=settings.TABLE_PARKS,
        gcs_bucket=settings.BUCKET_NAME,
        gcs_path_prefix=settings.PATH_PARKS,
        # Define the columns inside the JSON
        data_schema="""
            id INT64,
            name STRING,
            parks JSON
        """,
        # Define the columns in the folder path
        partition_schema="""
            year STRING,
            month STRING,
            day STRING,
            hour STRING,
            minute STRING
        """
    )
    
    # ---------------------------------------------------------
    # 2. Queue Times
    # ---------------------------------------------------------
    logger.info("Configuring Queue Times table...")
    bq.create_external_table_via_sql(
        dataset_id=settings.DATASET_NAME,
        table_id=settings.TABLE_QUEUES,
        gcs_bucket=settings.BUCKET_NAME,
        gcs_path_prefix=settings.PATH_QUEUES,
        # Define the columns inside the JSON
        data_schema="""
            park_id INT64,
            lands JSON,
            rides JSON
        """,
        # Define the columns in the folder path
        partition_schema="""
            year STRING,
            month STRING,
            day STRING,
            hour STRING,
            minute STRING
        """
    )
    
    logger.info(f"Infra setup completed successfully")

if __name__ == "__main__":
    infrastructure_setup()