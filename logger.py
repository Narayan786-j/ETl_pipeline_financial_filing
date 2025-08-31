import logging

# Configure logger (only once in your main script)
logging.basicConfig(
    level=logging.DEBUG,  # levels: DEBUG, INFO, WARNING, ERROR, CRITICAL
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("etl_debug.log"),   # log to file
        logging.StreamHandler()                 # also log to console
    ]
)

logger = logging.getLogger(__name__)