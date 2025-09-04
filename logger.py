import logging
import os

# Create logs directory if it doesn't exist
if not os.path.exists("logs"):
    os.makedirs("logs")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s — %(name)s — %(levelname)s — %(message)s",
    handlers=[
        logging.FileHandler("logs/project.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger("project_logger")
