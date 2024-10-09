from dataclasses import dataclass
from time import time
import json
import os

from pathlib import Path

from dotenv import load_dotenv
from web3 import Web3

load_dotenv()

eth_client = Web3(Web3.HTTPProvider(os.environ["ALCHEMY_URL"]))


AUTO_ETH_BUCKET = os.environ["AUTO_ETH_BUCKET"]
BAL_ETH_BUCKET = os.environ["BAL_ETH_BUCKET"]
AUTO_LRT_BUCKET = os.environ["AUTO_LRT_BUCKET"]
ALCHEMY_URL = os.environ["ALCHEMY_URL"]


CACHE_TIME = 3600 * 6  # siz hours

ROOT_DIR = Path(__file__).parent
SOLVER_REBALANCE_PLANS_DIR = ROOT_DIR / "rebalance_plans"

if not os.path.exists(SOLVER_REBALANCE_PLANS_DIR):
    os.makedirs(SOLVER_REBALANCE_PLANS_DIR)


def time_decorator(func):
    def wrapper(*args, **kwargs):
        start_time = time()
        result = func(*args, **kwargs)
        elapsed_time = time() - start_time
        print(f"{func.__name__} took {elapsed_time:.4f} seconds.")
        return result

    return wrapper


@dataclass
class AutopoolConstants:
    name: str
    autopool_eth_addr: str
    autopool_eth_strategy_addr: str
    solver_rebalance_plans_bucket: str


# mainnet as of sep 16, 2024
SYSTEM_REGISTRY = "0x2218F90A98b0C070676f249EF44834686dAa4285 "
AUTOPOOL_REGISTRY = "0x7E5828a3A6Ae75426d739E798140513A2E2964E4"
ROOT_PRICE_ORACLE = "0x61F8BE7FD721e80C0249829eaE6f0DAf21bc2CaC"
LENS_CONTRACT = "0x146b5564dd061D648275e4Bd3569b8c285783882"

AUTO_ETH = AutopoolConstants(
    "autoETH",
    "0x0A2b94F6871c1D7A32Fe58E1ab5e6deA2f114E56",
    "0xf5f6addB08c5e6091e5FdEc7326B21bEEd942235",
    AUTO_ETH_BUCKET,
)

BAL_ETH = AutopoolConstants(
    "balETH", "0x6dC3ce9C57b20131347FDc9089D740DAf6eB34c5", "0xabe104560D0B390309bcF20b73Dca335457AA32e", BAL_ETH_BUCKET
)

AUTO_LRT = AutopoolConstants(
    "autoLRT",
    "0xE800e3760FC20aA98c5df6A9816147f190455AF3",
    "0x72a726c10220280049687E58B7b05fb03d579109",
    AUTO_LRT_BUCKET,
)

ALL_AUTOPOOLS = [AUTO_ETH, BAL_ETH, AUTO_LRT]
AUTOPOOL_NAME_TO_CONSTANTS = {a.name: a for a in ALL_AUTOPOOLS}


STREAMLIT_MARKDOWN_HTML = """
        <style>
        .main {
            max-width: 85%;
            margin: 0 auto;
            padding-top: 40px;
        }
        .stPlotlyChart {
            width: 100%;
            height: auto;
            min-height: 300px;
            max-height: 600px;
            background-color: #f0f2f6;
            border-radius: 5px;
            padding: 20px;
        }
        @media (max-width: 768px) {
            .stPlotlyChart {
                min-height: 250px;
                max-height: 450px;
            }
        }
        .stPlotlyChart {
            background-color: #f0f2f6;
            border-radius: 5px;
            padding: 10px;
        }
        .stExpander {
            background-color: #e6e9ef;
            border-radius: 5px;
            padding: 10px;
        }
        </style>
        """
