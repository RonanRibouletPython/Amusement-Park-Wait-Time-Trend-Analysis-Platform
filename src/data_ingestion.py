import requests
import json
from pathlib import Path
from datetime import datetime
import pandas as pd
from tenacity import retry, stop_after_attempt, wait_fixed

from tools.logger import get_logger

logger = get_logger(__name__)

class DataIngestion():
    
    def __init__(self):
        # CONSTANTS
        self.BASE_API_URL = "https://queue-times.com/"
        self.PARKS_URL = self.BASE_API_URL + "parks.json"
        self.QUEUE_TIMES_URL = self.BASE_API_URL + "parks/{}/queue_times.json"

# Fetch the list of parks
    @retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
    def fetch_parks(self)-> dict|None:
        logger.info("Fetching list of parks")
        try:
            logger.info(f"Fetching list of parks from {self.PARKS_URL}")
            # Send a GET request to the API
            response = requests.get(self.PARKS_URL)
            # Check if the request was successful
            response.raise_for_status()
            logger.info(f"Response status code: {response.status_code}")
            # Return the list of parks as raw JSON
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch parks: {e}")
            return None
        except Exception as e:
            logger.error(f"An unexpected error occurred: {e}")
            return None

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
    def fetch_queue_times(self, park_id: str) -> dict|None:
        logger.info(f"Fetching queue times for park with park_id {park_id}")

        # Fetch the queue times
        try:
            logger.info(f"Fetching queue times from {self.QUEUE_TIMES_URL.format(park_id)}")
            # Send a GET request to the API
            response = requests.get(self.QUEUE_TIMES_URL.format(park_id))
            # Check if the request was successful
            response.raise_for_status()
            logger.info(f"Response status code: {response.status_code}")
            # Return the list of queue times as raw JSON
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch queue times: {e}")
            return None
        except Exception as e:
            logger.error(f"An unexpected error occurred: {e}")
            return None

# Test the function for debugging
if __name__ == "__main__":
    data_ingestion = DataIngestion()
    
    # parks = data_ingestion.fetch_parks()
    # print(parks)

    # queue_times = data_ingestion.fetch_queue_times(2)
    # print(queue_times)
    