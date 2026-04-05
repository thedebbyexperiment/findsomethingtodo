import os
from dotenv import load_dotenv

load_dotenv(override=True)

SEATGEEK_CLIENT_ID = os.getenv("SEATGEEK_CLIENT_ID", "")
EVENTBRITE_TOKEN = os.getenv("EVENTBRITE_TOKEN", "")
TICKETMASTER_API_KEY = os.getenv("TICKETMASTER_API_KEY", "")
GOOGLE_PLACES_API_KEY = os.getenv("GOOGLE_PLACES_API_KEY", "")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")

DB_PATH = os.path.join(os.path.dirname(__file__), "db", "findsomethingtodo.db")
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "output")

# NYC bounding box for geo queries
NYC_LAT = 40.7128
NYC_LNG = -74.0060
NYC_RADIUS_KM = 30

# Rate limiting defaults (seconds between requests)
DEFAULT_RATE_LIMIT = 1.0
SCRAPE_RATE_LIMIT = 2.0

# LLM normalization
LLM_MODEL = "claude-haiku-4-5-20251001"
LLM_BATCH_SIZE = 10
