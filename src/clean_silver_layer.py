from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from pydantic_settings import BaseSettings
from tools.logger import get_logger
import ibis
from ibis import _

logger = get_logger(__name__)

class Settings(BaseSettings):
    GCP_PROJECT_ID: str = "amusement-park-wait-time"
    RAW_DATASET: str = "amusement_park_raw" # Source 
    DERIVED_DATASET: str = "amusement_park_derived" # Destination
    
    RAW_TABLE: str = "queue_times"
    SILVER_TABLE: str = "queue_times_cleaned"
    
    LOCATION: str = "EU"

settings = Settings()

class SilverLayer():
    def __init__(self):
        # Init the BigQuery client
        self.client = bigquery.Client(project=settings.GCP_PROJECT_ID)
        # Init the Ibis connection
        self.con = ibis.bigquery.connect(
            project_id=settings.GCP_PROJECT_ID,
            dataset_id=settings.RAW_DATASET
        )
        
    def setup_silver_dataset(self):
        
        dataset_id = f"{settings.GCP_PROJECT_ID}.{settings.DERIVED_DATASET}"
        
        # Ensure the destination dataset exists
        try:
            self.client.get_dataset(dataset_id)
            logger.info(f"Dataset {settings.DERIVED_DATASET} already exists")
        except NotFound:
            logger.info(f"Dataset not found... Creating dataset {settings.DERIVED_DATASET}")
            dataset = bigquery.Dataset(dataset_id)
            dataset.location = settings.LOCATION
            self.client.create_dataset(dataset)
            logger.info(f"Dataset creates successfully")
    
    def transform_and_load(self):
        logger.info(f"Use Ibis to transform data...")
        
        # Point to the source table
        t = self.con.table(settings.RAW_TABLE, database=settings.RAW_DATASET)
        
        # Structure the Timestamp from Hive partition strings
        # Form: "YYYY-MM-DD HH:mm:SS"
        ts_string_expression = (
            t.year + "-" + t.month + "-" + t.day + " " +
            t.hour + ":" + t.minute + ":00"
        )
        
        silver_plan = (
            t.mutate(
                # Create the timestamp col
                timestamp=ts_string_expression.cast("timestamp"),
                # Make sure Park ID is an Integer
                park_id=t.park_id.cast("int64")
        )
        # Select + Reorder columns
        .select(
            "timestamp",
            "park_id",
            "lands",
            "rides",
            "year",
            "month",
            "day",
            "hour",
            "minute"
        )
        # Filter out bad rows
        .filter(
            _.year.notnull()
        )
        .order_by(
            ibis.desc("timestamp"))
        )
        
        # Check the query created
        logger.info(f"Generated SQL:\n{ibis.to_sql(silver_plan)}")
        
        # Execute and write the destination table
        dest_table_id = f"{settings.GCP_PROJECT_ID}.{settings.DERIVED_DATASET}.{settings.SILVER_TABLE}"
        logger.info(f"Writing in table {dest_table_id}")
        
        # Using Ibis to write the table directly
        # overwrite=True acts like CREATE OR REPLACE TABLE
        self.con.create_table(
            settings.SILVER_TABLE,
            obj=silver_plan,
            database=settings.DERIVED_DATASET,
            overwrite=True
        )
        
        logger.info("Silver Layer transformation complete!")
    
    def run_pipeline(self):
        self.setup_silver_dataset()
        self.transform_and_load()

if __name__ == "__main__":
    job = SilverLayer()
    job.run_pipeline()
        
        
        
        