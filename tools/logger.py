import logging
import sys
from pathlib import Path

# Define path for the logs
LOG_DIR = Path(__file__).parent.parent / "logs"
LOG_DIR.mkdir(exist_ok=True)

# Create logger
def get_logger(name: str)-> logging.Logger:
    # Create an instance of the logger
    logger = logging.getLogger(name)
    
    if logger.hasHandlers():
        return logger
    
    # Set the lowest level of sevirity to lowest to capture all logs
    logger.setLevel(logging.DEBUG)
    
    # Define the format for the logs
    formatter = logging.Formatter(
        fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    
    # Create a handler for the logs
    file_handler = logging.FileHandler(LOG_DIR / f"{name}.log")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    
    # Create a handler for the console
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.INFO)
    
    # Add the configured handlers to the logger
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    
    return logger