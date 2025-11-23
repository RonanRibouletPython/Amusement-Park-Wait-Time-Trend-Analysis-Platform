from typing import List, Optional
from google.cloud import bigquery
from google.api_core.exceptions import Conflict, NotFound
from tools.logger import get_logger

logger = get_logger(__name__)

class BigQueryHandler:
    """
    Handles interactions with Google BigQuery.
    Focuses on Dataset management and External Table linking.
    """
    def __init__(self, project_id: str, location: str = "EU") -> None:
        self.project_id = project_id
        self.location = location
        self.client = bigquery.Client(project=project_id, location=location)
        
    def create_dataset_if_not_exists(self, dataset_id: str) -> None:
        """
        Creates a BigQuery Dataset if it doesn't exist.
        """
        dataset_reference = f"{self.project_id}.{dataset_id}"
        
        try:
            self.client.get_dataset(dataset_reference)
            logger.info(f"Dataset {dataset_reference} already exists")
        except NotFound:
            # If the dataset is not found let's create it
            dataset = bigquery.Dataset(dataset_reference)
            dataset.location = self.location
            self.client.create_dataset(dataset)
            logger.info(f"Dataset {dataset_reference} created successfully")

    def execute_ddl(self, ddl: str) -> None:
        """Executes a SQL Data Definition statement"""
        query_job = self.client.query(ddl)
        query_job.result() # Wait for job to complete
        logger.info("DDL Executed successfully.")

    def create_external_table_via_sql(
        self,
        dataset_id: str,
        table_id: str,
        gcs_bucket: str,
        gcs_path_prefix: str,
        data_schema: str,      # SQL definition for data cols: "id INT64, name STRING"
        partition_schema: str  # SQL definition for partition cols: "year STRING, month STRING"
    ) -> None:
        """
        Creates an external table using Raw SQL DDL.
        This allows strict separation of Data Columns vs Partition Columns.
        """
        full_table_id = f"{self.project_id}.{dataset_id}.{table_id}"
        uri = f"gs://{gcs_bucket}/{gcs_path_prefix}*"
        hive_prefix = f"gs://{gcs_bucket}/{gcs_path_prefix}"
        
        # Construct the SQL Statement exactly as it worked in the console
        ddl = f"""
        CREATE OR REPLACE EXTERNAL TABLE `{full_table_id}`
        (
            {data_schema}
        )
        WITH PARTITION COLUMNS (
            {partition_schema}
        )
        OPTIONS (
            format = 'NEWLINE_DELIMITED_JSON',
            uris = ['{uri}'],
            hive_partition_uri_prefix = '{hive_prefix}',
            ignore_unknown_values = TRUE
        );
        """
        
        logger.info(f"Creating External Table {table_id} via SQL...")
        self.execute_ddl(ddl)