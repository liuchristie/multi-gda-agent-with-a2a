import logging
import os

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

ROUTER_MODEL = os.environ.get("ROUTER_MODEL", "gemini-3.5-flash-lite")
A2A_HISTORY_LENGTH = int(os.environ.get("A2A_HISTORY_LENGTH", 10))

GDA_AGENTS_BILLING_PROJECT = os.environ.get(
    "GDA_AGENTS_BILLING_PROJECT",
    os.environ.get("AGENTS_BILLING_PROJECT", "liuchristie-142-20250922213141"),
)
AGENTS_BILLING_PROJECT = GDA_AGENTS_BILLING_PROJECT
DATA_AGENT_BILLING_PROJECT = GDA_AGENTS_BILLING_PROJECT

GDA_AGENTS_LOCATION = os.environ.get(
    "GDA_AGENTS_LOCATION",
    os.environ.get("AGENTS_LOCATION", "global"),
)
AGENTS_LOCATION = GDA_AGENTS_LOCATION
DATA_AGENT_LOCATION = GDA_AGENTS_LOCATION

AGENTS_FILTER = os.environ.get("AGENTS_FILTER", 'labels.team = "data-analytics"')





