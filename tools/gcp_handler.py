from google.cloud import storage
from google.api_core import exceptions

from .logger import get_logger

# Initialize the logger for this module
logger = get_logger(__name__)

class GCSHandler:
    """
    Class for handling Google Cloud Storage (GCS) operations
    
    """
    
    def __init__(self, project_id: str, bucket_name: str, location: str = "EU") -> None:
        """
        Initialize the GCSHandler class
        
        """
        
        if not project_id:
            raise ValueError("Project ID is required")
        
        if not bucket_name:
            raise ValueError("Bucket name is required")
        
        self.project_id = project_id
        self.bucket_name = bucket_name
        self.location = location
        
        # Create a single instance of the GCS client for use throughout the class
        self.client = storage.Client(project=self.project_id)
        
        # Bucket logic
        self.bucket = self.create_or_get_bucket()
        logger.info(f"GSCHandler initialized for project {self.project_id} and bucket {self.bucket_name}")
        
    
    def create_or_get_bucket(self) -> storage.Bucket:
        """
        Create or retrieve the GCS bucket
        
        """
        # Check if the bucket already exists
        try:
            bucket = self.client.get_bucket(self.bucket_name)
            logger.info(f"Bucket {self.bucket_name} already exists")
        except exceptions.NotFound:
            # If not we create a new bucket
            logger.info(f"Bucket '{self.bucket_name}' not found. Attempting to create it...")
            new_bucket = self.client.create_bucket(self.bucket_name, location=self.location)
            logger.info(f"Bucket '{self.bucket_name}' created successfully for location {self.location}")
        
            return new_bucket
    
    def upload_json_to_gcs(self, bucket_name: str, json_data: dict, blob_name: str):
        pass