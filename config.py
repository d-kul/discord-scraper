import logging
import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

RESOURCES_PATH = Path(os.environ.get("RESOURCES_PATH", "resources"))
DATA_PATH = Path(os.environ.get("DATA_PATH", "data"))
LOG_LEVEL = logging.getLevelName(os.environ.get("LOG_LEVEL", "INFO"))

RESOURCES = {
    "NUH_UH": RESOURCES_PATH / "nuh_uh.txt",
    "SKIP_CHANNELS": RESOURCES_PATH / "skip_channels.txt",
}

DATA = {
    "MEMBERS": DATA_PATH / "members.csv",
    "CHANNELS": DATA_PATH / "channels.csv",
    "MESSAGES": DATA_PATH / "messages.csv",
    "REACTIONS": DATA_PATH / "reactions.csv",
    "CHECKPOINT": DATA_PATH / "checkpoint.json",
}

BOT = {
    "TOKEN": os.environ["TOKEN"],
    "ADMIN_ID": int(os.environ["ADMIN_ID"]),
}

SCRAPER = {
    "BUFFER_SIZE": 500,
    "REQUESTS_PER_PERIOD": 5,
    "PERIOD_SECONDS": 5,
    "ERROR_RESTART_SECONDS": 10,
}
