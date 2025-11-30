from dagster import ConfigurableResource
from pyspark.sql import SparkSession

class SparkManager(ConfigurableResource):
    """
    Wraps the SparkSession setup to be used as a Dagster Resource

    """
    
    project_id: str
    staging_bucket: str
    
    def get_spark_session(self, app_name: str = "AmusementPark_ELT") -> SparkSession:
        
        # Step 1: Define the BigQuery Connector JAR version
        # Using 2.12 because of its ultra compatibility
        # See documentation: https://github.com/GoogleCloudDataproc/spark-bigquery-connector
        bq_connector_jar = "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.34.0"
        
        # Step 2: Build the session
        builder = (
            SparkSession.builder
            # Sets a name for the application 
            .appName(app_name)
            # Sets a config option
            .config("spark.jars.packages", bq_connector_jar)
            # Temporary GCS bucket required for writing to BigQuery
            .config("temporaryGcsBucket", self.staging_bucket)
            # Ensure the project ID is set for billing/quota
            .config("parentProject", self.project_id)
            # Add a tag to the session
            .addTag("dagster")
        )
        
        # Step 3: Create or retrieve existing
        spark = builder.getOrCreate()
        
        # Reduce log verbosity (Spark is very noisy by default)
        spark.sparkContext.setLogLevel("WARN")
        
        return spark