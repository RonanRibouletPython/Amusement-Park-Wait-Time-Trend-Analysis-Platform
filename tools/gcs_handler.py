from google.cloud import storage
from google.api_core.exceptions import Conflict
import asyncio
import json
from typing import Any, Optionnal


from tools.logger import get_logger

# Initialize the logger for this module
logger = get_logger(__name__)

class GCSHandler:
    """
    Handles Google Cloud Storage interactions
    Wraps blocking Google calls in asyncio executors for non-blocking performance
    
    """
    
    def __init__(self, project_id: str, bucket_name: str) -> None:
        self.project_id = project_id
        self.bucket_name = bucket_name
        self.client = storage.Client(project=self.project_id)
        self.bucket = self.client.bucket(self.bucket_name)
        
    async def create_bucket_if_not_exists(self, location: str = "EU") -> None:
        """
        Creates the bucket if it does not exist
        """
        loop = asyncio.get_running_loop()
        # Run blocking network call in a thread
        await loop.run_in_executor(None, self._create_bucket_sync, location)
        
    def _create_bucket_sync(self, location: str) -> None:
        try:
            if not self.bucket.exists():
                self.bucket = self.client.create_bucket(self.bucket_name, location=location)
                logger.info(f"Bucket {self.bucket_name} created in {location}")
            else:
                logger.info(f"Bucket {self.bucket_name} already exists")
        except Conflict:
            # Race condition handling if two scripts try to create a bucket at the same time
            logger.warning(f"Bucket '{self.bucket_name}' already owned.")
        except Exception as e:
            logger.error(f"Failed to create bucket: {e}")
            raise e