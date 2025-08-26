import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()


ALCHEMY_URL = os.getenv("ALCHEMY_URL")
ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY")
ETHERSCAN_API_URL = "https://api.etherscan.io/v2/api"

COINGECKO_API_KEY = os.getenv("COINGECKO_API_KEY")
DEFAULT_GAS_LIMIT = int(os.getenv("GAS_LIMIT", "550000000"))

TOKEMAK_SUBGRAPH_URLS = {
    "eth": os.environ.get("TOKEMAK_ETHEREUM_SUBGRAPH_URL"),
    "base": os.environ.get("TOKEMAK_BASE_SUBGRAPH_URL"),
    "sonic": os.environ.get("TOKEMAK_SONIC_SUBGRAPH_URL"),
}

BUCKETS = {
    "AUTO_ETH": os.environ.get("AUTO_ETH_BUCKET"),
    "BAL_ETH": os.environ.get("BAL_ETH_BUCKET"),
    "AUTO_LRT": os.environ.get("AUTO_LRT_BUCKET"),
    "BASE_ETH": os.environ.get("BASE_ETH_BUCKET"),
    "DINERO_ETH": os.environ.get("DINERO_ETH_BUCKET"),
    "AUTO_USD": os.environ.get("AUTO_USD_BUCKET"),
    "BASE_USD": os.environ.get("BASE_USD_BUCKET"),
    "AUTO_DOLA": os.environ.get("AUTO_DOLA_BUCKET"),
    "SONIC_USD": os.environ.get("SONIC_USD_BUCKET"),
    "BASE_EUR": os.environ.get("BASE_EUR_BUCKET"),
    "SILO_USD": os.environ.get("SILO_USD_BUCKET"),
    "SILO_ETH": os.environ.get("SILO_ETH_BUCKET"),
}


ROOT_DIR = Path(__file__).parent.parent.parent.resolve()
SOLVER_REBALANCE_PLANS_DIR = ROOT_DIR / "data_fetching/rebalance_plans"
WORKING_DATA_DIR = ROOT_DIR / "working_data"

# TODO these can all be removed
DB_DIR = ROOT_DIR / "database"
DB_FILE = DB_DIR / "autopool_dashboard.db"
PRODUCTION_LOG_FILE_NAME = "production_usage.log"
TEST_LOG_FILE_NAME = "test_pages.log"
STARTUP_LOG_FILE = ROOT_DIR / "startup.csv"
