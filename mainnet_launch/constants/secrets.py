from pathlib import Path

from dotenv import load_dotenv
from os import environ

load_dotenv()

if environ["WHICH_ALCHEMY_URL"] == "ANALYTICS_DEV2_ALCHEMY_URL":
    ALCHEMY_URL = environ["ANALYTICS_DEV2_ALCHEMY_URL"]
elif environ["WHICH_ALCHEMY_URL"] == "AUTOPOOL_DASHBOARD_CI_ALCHEMY_URL":
    ALCHEMY_URL = environ["AUTOPOOL_DASHBOARD_CI_ALCHEMY_URL"]
else:
    raise ValueError(f"Unknown WHICH_ALCHEMY_URL value: {environ['WHICH_ALCHEMY_URL']}")

ETHERSCAN_API_KEY = environ["ETHERSCAN_API_KEY"]
ETHERSCAN_API_URL = "https://api.etherscan.io/v2/api"

COINGECKO_API_KEY = environ["COINGECKO_API_KEY"]
DEFAULT_GAS_LIMIT = 550_000_000

TOKEMAK_SUBGRAPH_URLS = {
    "eth": environ["TOKEMAK_ETHEREUM_SUBGRAPH_URL"],
    "base": environ["TOKEMAK_BASE_SUBGRAPH_URL"],
    "sonic": environ["TOKEMAK_SONIC_SUBGRAPH_URL"],
    "arb": environ["TOKEMAK_ARBITRUM_SUBGRAPH_URL"],
    "plasma": environ["TOKEMAK_PLASMA_SUBGRAPH_URL"],
}

BUCKETS = {
    "AUTO_ETH": environ["AUTO_ETH_BUCKET"],
    "BAL_ETH": environ["BAL_ETH_BUCKET"],
    "AUTO_LRT": environ["AUTO_LRT_BUCKET"],
    "BASE_ETH": environ["BASE_ETH_BUCKET"],
    "DINERO_ETH": environ["DINERO_ETH_BUCKET"],
    "AUTO_USD": environ["AUTO_USD_BUCKET"],
    "BASE_USD": environ["BASE_USD_BUCKET"],
    "AUTO_DOLA": environ["AUTO_DOLA_BUCKET"],
    "SONIC_USD": environ["SONIC_USD_BUCKET"],
    "BASE_EUR": environ["BASE_EUR_BUCKET"],
    "SILO_USD": environ["SILO_USD_BUCKET"],
    "SILO_ETH": environ["SILO_ETH_BUCKET"],
    "ARB_USD": environ["ARB_USD_BUCKET"],
    "PLASMA_USD": environ["PLASMA_USD_BUCKET"],
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

SEMAPHORE_LIMITS_FOR_MULTICALL = tuple(
    int(x) for x in environ.get("SEMAPHORE_LIMITS_FOR_MULTICALL", "100,20,1").split(",")
)

V2_DASHBOARD_NOTIFS_WEBHOOK_URL = environ.get("V2_DASHBOARD_NOTIFS_WEBHOOK_URL")
