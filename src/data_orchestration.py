from tools.logger import get_logger
import asyncio
from data_ingestion import DataIngestion
import httpx

logger = get_logger(__name__)

class DataOrchestration:
    def __init__(self, data_ingestion: "DataIngestion") -> None:
        self.data_ingestion = data_ingestion
        
        logger.info("DataOrchestration initialized")
    
    async def run_pipeline(self) -> None:
        logger.info("Starting data ingestion pipeline")
        
        async with httpx.AsyncClient(timeout=10.0) as client:
            # 1. Get list of the parks
            parks = await self.data_ingestion.process_parks_metadata(client)

            if not parks:
                logger.error("Failed to fetch parks metadata.")
                return
            logger.info(f"Success to fetch parks metadata")
            
            # 2. Extract IDs from the parks list
            park_ids = []
            for group in parks:
                # Check if this group has a 'parks' list (Group or Company)
                if "parks" in group:
                    group_name = group.get("name", "Unknown Group")
                    for park in group["parks"]:
                        if "id" in park:
                            park_ids.append(park["id"])
                # flat park at the root level (Standalone park)
                elif "id" in group:
                    park_ids.append(group["id"])
            
            # Data validation
            logger.info("Parsing task complete here are some info:")
            logger.info(f"Total Individual Parks found: {len(park_ids)}")
            logger.info(f"Sample IDs: {park_ids[:5]}...")
            
            # 3. Create the async task for queue times
            tasks = []
            for park_id in park_ids:
                tasks.append(self.data_ingestion.process_single_queue_time(client, park_id))
            
            # 4. Run all tasks
            logger.info(f"Starting the async fetch for {len(tasks)} parks")
            results = await asyncio.gather(*tasks)
            
            # Result validation
            valid_results = [r for r in results if r]
            logger.info(f"Pipeline finished. Successfully retrieved {len(valid_results)}/{len(park_ids)} queue datasets.")
        
        logger.info(f"Pipeline finished successfully with {len(park_ids)} parks")

if __name__ == "__main__":
    ingestion = DataIngestion()
    orchestrator = DataOrchestration(ingestion)
    asyncio.run(orchestrator.run_pipeline())