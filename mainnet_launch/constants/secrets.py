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

ALCHEMY_API_KEY = ALCHEMY_URL.split("/")[-1]

ETHERSCAN_API_KEY = environ["ETHERSCAN_API_KEY"]
ETHERSCAN_API_URL = "https://api.etherscan.io/v2/api"

COINGECKO_API_KEY = environ["COINGECKO_API_KEY"]
DEFAULT_GAS_LIMIT = 550_000_000

S3_BUCKETS = {
    "AUTO_ETH": environ["AUTO_ETH_BUCKET"],  # Before Jan 2, 2026  # slight overlap 1 plan Jan 03, 2026 here
    "AUTO_ETH2": environ["AUTO_ETH_BUCKET2"],  # after Jan 2, 2026
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
    "LINEA_USD": environ["LINEA_USD_BUCKET"],
    "ANCHRG_USD": environ["ANCHRG_USD_BUCKET"],
}

ROOT_DIR = Path(__file__).parent.parent.parent.resolve()
SOLVER_REBALANCE_PLANS_DIR = ROOT_DIR / "data_fetching/rebalance_plans"
WORKING_DATA_DIR = ROOT_DIR / "working_data"

SEMAPHORE_LIMITS_FOR_MULTICALL = tuple(
    int(x) for x in environ.get("SEMAPHORE_LIMITS_FOR_MULTICALL", "100,20,1").split(",")
)
