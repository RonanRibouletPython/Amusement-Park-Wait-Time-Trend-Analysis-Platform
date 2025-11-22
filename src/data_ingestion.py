import asyncio
import json
from typing import List, Dict, Any, Optional
import httpx
from pydantic_settings import BaseSettings
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type, before_log

from tools.logger import get_logger

logger = get_logger(__name__)

class Settings(BaseSettings):
    # BASE API URL
    BASE_API_URL: str = "https://queue-times.com/"
    PARKS_ENDPOINT: str = "parks.json"
    QUEUE_TIMES_ENDPOINT: str = "parks/{park_id}/queue_times.json"
    
    # CONCURRENCY LIMIT
    CONCURRENCY_LIMIT: int = 10

settings = Settings()

class DataIngestion():
    def __init__(self):
        # Semaphore to control concurrency
        self.semaphore = asyncio.Semaphore(settings.CONCURRENCY_LIMIT)

# Fetch the list of parks
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_fixed(2),
        retry=retry_if_exception_type(httpx.RequestError),
        before=before_log(logger=logger, log_level=1),
        )
    async def _fetch_url(self, client: httpx.AsyncClient, url: str) -> Any:
        """Fetch a URL asynchronously with retries"""
        logger.info(f"Fetching URL: {url}")
        response = await client.get(url)
        response.raise_for_status()
        return response.json()
    
    
    async def process_parks_metadata(self, client: httpx.AsyncClient) -> List[Dict[str, Any]]:
        # Construct the URL
        url = settings.BASE_API_URL + settings.PARKS_ENDPOINT
        logger.info(f"Fetching URL: {url}")

        try:
            data = await self._fetch_url(client, url)
            # Return the list of parks as raw JSON
            return data
        except Exception as e:
            logger.error(f"An unexpected error occurred: {e}")
            return []
        
    async def process_single_queue_time(self, client: httpx.AsyncClient, park_id: int) -> List[Dict[str, Any]]:
        # Construct the URL
        url = settings.BASE_API_URL + settings.QUEUE_TIMES_ENDPOINT.format(park_id=park_id)
        logger.info(f"Fetching URL: {url}")
        
        async with self.semaphore:
            try:
                data = await self._fetch_url(client, url)
                
                if isinstance(data, dict):
                    data["park_id"] = park_id
                
                return data
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 404:
                    logger.warning(f"Park with ID {park_id} not found. Skipping.")
                    return []
                else:
                    logger.error(f"Fetch queue faile for park ID {park_id}: {e}")
            except Exception as e:
                logger.error(f"An unexpected error occurred for park ID {park_id}: {e}")
    