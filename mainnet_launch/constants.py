from dataclasses import dataclass
from time import time
from enum import Enum
import os

from pathlib import Path

from dotenv import load_dotenv
from web3 import Web3
from web3.middleware import geth_poa_middleware
from typing import ClassVar

load_dotenv()

eth_client = Web3(Web3.HTTPProvider(os.environ["ALCHEMY_URL"]))
base_client = Web3(Web3.HTTPProvider(os.environ["ALCHEMY_URL"].replace("eth-mainnet", "base-mainnet")))
base_client.middleware_onion.inject(geth_poa_middleware, layer=0)


WEB3_CLIENTS: dict[str, Web3] = {
    "eth": eth_client,
    "base": base_client,
}

CACHE_TIME = 3600 * 6  # six hours
ROOT_DIR = Path(__file__).parent
SOLVER_REBALANCE_PLANS_DIR = ROOT_DIR / "rebalance_plans"
WORKING_DATA_DIR = ROOT_DIR / "working_data"
TX_HASH_TO_GAS_COSTS_PATH = WORKING_DATA_DIR / "tx_hash_to_gas_info.json"
DB_DIR = ROOT_DIR / "databases"


os.makedirs(SOLVER_REBALANCE_PLANS_DIR, exist_ok=True)
os.makedirs(WORKING_DATA_DIR, exist_ok=True)
os.makedirs(DB_DIR, exist_ok=True)

if not os.path.exists(TX_HASH_TO_GAS_COSTS_PATH):
    open(TX_HASH_TO_GAS_COSTS_PATH, "x").close()  # create an empty file if it does not exist


@dataclass(frozen=True)
class ChainData:
    name: str
    block_autopool_first_deployed: int
    approx_seconds_per_block: float
    chain_id: int

    def __hash__(self):
        return self.chain_id

    @property
    def client(self) -> Web3:
        """
        Dynamically retrieves the Web3 client associated with this chain.

        This is required to ensure that ChainData is hashable so can be used in

        @st.cache_data(ttl=CACHE_TIME)

        """
        if self.name not in WEB3_CLIENTS:
            raise ValueError(f"No Web3 client configured for chain: {self.name}")
        return WEB3_CLIENTS[self.name]


@dataclass(frozen=True)
class AutopoolConstants:
    name: str
    autopool_eth_addr: str
    autopool_eth_strategy_addr: str
    solver_rebalance_plans_bucket: str
    chain: ChainData

    def __hash__(self):
        return hash(self.chain)


ETH_CHAIN: ChainData = ChainData(
    name="eth", block_autopool_first_deployed=20752910, approx_seconds_per_block=12.0, chain_id=1
)
BASE_CHAIN: ChainData = ChainData(
    name="base",
    block_autopool_first_deployed=21241103,
    approx_seconds_per_block=2.0,
    chain_id=8453,
)


AUTO_ETH: AutopoolConstants = AutopoolConstants(
    name="autoETH",  #  "Tokemak autoETH",
    autopool_eth_addr="0x0A2b94F6871c1D7A32Fe58E1ab5e6deA2f114E56",
    autopool_eth_strategy_addr="0xf5f6addB08c5e6091e5FdEc7326B21bEEd942235",
    solver_rebalance_plans_bucket=os.environ["AUTO_ETH_BUCKET"],
    chain=ETH_CHAIN,
)

BAL_ETH: AutopoolConstants = AutopoolConstants(
    name="balETH",  #  "Tokemak autoETH",
    autopool_eth_addr="0x6dC3ce9C57b20131347FDc9089D740DAf6eB34c5",
    autopool_eth_strategy_addr="0xabe104560D0B390309bcF20b73Dca335457AA32e",
    solver_rebalance_plans_bucket=os.environ["BAL_ETH_BUCKET"],
    chain=ETH_CHAIN,
)

AUTO_LRT: AutopoolConstants = AutopoolConstants(
    name="autoLRT",  # "Tokemak autoLRT"
    autopool_eth_addr="0xE800e3760FC20aA98c5df6A9816147f190455AF3",
    autopool_eth_strategy_addr="0x72a726c10220280049687E58B7b05fb03d579109",
    solver_rebalance_plans_bucket=os.environ["AUTO_LRT_BUCKET"],
    chain=ETH_CHAIN,
)

BASE_ETH: AutopoolConstants = AutopoolConstants(
    "baseETH",  # "Tokemak baseETH"
    autopool_eth_addr="0xAADf01DD90aE0A6Bb9Eb908294658037096E0404",
    autopool_eth_strategy_addr="0xe72a466d426F735BfeE91Db19dc509735B65b8dc",
    solver_rebalance_plans_bucket=os.environ["BASE_ETH_BUCKET"],
    chain=BASE_CHAIN,
)


ALL_AUTOPOOLS: list[AutopoolConstants] = [AUTO_ETH, BAL_ETH, AUTO_LRT, BASE_ETH]


@dataclass
class TokemakAddress:
    """For contracts that exist both on Ethereum and Base"""

    eth: str
    base: str

    def __post_init__(self):
        if not Web3.isChecksumAddress(self.eth):
            raise ValueError(f"{self.eth} must be a checksum address")

        if not Web3.isChecksumAddress(self.base):
            raise ValueError(f"{self.base} must be a checksum address should be: {Web3.toChecksumAddress(self.base)=}")

    def __call__(self, chain: ChainData) -> str:
        """
        Returns the contract address for the specified chain.
        Raises ValueError if the address is not defined for the given chain.
        """
        if chain.name == "eth":
            return self.eth
        elif chain.name == "base":
            return self.base
        else:
            raise ValueError(f"No address defined for chain: {chain.name}")


SYSTEM_REGISTRY = TokemakAddress(
    eth="0x2218F90A98b0C070676f249EF44834686dAa4285", base="0x18Dc926095A7A007C01Ef836683Fdef4c4371b4e"
)

AUTOPOOL_REGISTRY = TokemakAddress(
    eth="0x7E5828a3A6Ae75426d739E798140513A2E2964E4", base="0x4fE7916A10B15DADEFc59D06AC81757112b1feCE"
)

ROOT_PRICE_ORACLE = TokemakAddress(
    eth="0x61F8BE7FD721e80C0249829eaE6f0DAf21bc2CaC", base="0xBCf67d1d643C53E9C2f84aCBd830A5EDC2661795"
)

LENS_CONTRACT = TokemakAddress(
    eth="0x146b5564dd061D648275e4Bd3569b8c285783882", base="0xaF05c205444c5884F53492500Bed22A8f617Aa9C"
)

DESTINATION_VAULT_REGISTRY = TokemakAddress(
    eth="0x3AaC1CE01127593CA0c7f87b1Aedb1E153e152aE", base="0xBBBB6E844EEd5952B44C2063670093E27E21735f"
)

INCENTIVE_PRICNIG_STATS = TokemakAddress(
    eth="0x8607bA6540AF378cbA64F4E3497FBb2d1385f862", base="0xF28213d5cbc9f4cfB371599D25E232978848090d"
)

LIQUIDATION_ROW = TokemakAddress(
    eth="0xBf58810BB1946429830C1f12205331608c470ff5", base="0xE2F00bbC3E5ddeCfBD95e618CE36b49F38881d4f"
)

# only autoLRT on mainnet uses points
POINTS_HOOK = TokemakAddress(
    eth="0xA386067eB5F7Dc9b731fe1130745b0FB00c615C3", base="0x000000000000000000000000000000000000dEaD"
)


def time_decorator(func):
    def wrapper(*args, **kwargs):
        start_time = time()
        result = func(*args, **kwargs)
        elapsed_time = time() - start_time
        print(f"{func.__name__} took {elapsed_time:.4f} seconds.")
        return result

    return wrapper


# @time_decorator
# def lo():
#     for _ in range(1_000):
#         eth_client = Web3(Web3.HTTPProvider(os.environ["ALCHEMY_URL"]))
#         base_client = Web3(Web3.HTTPProvider(os.environ["ALCHEMY_URL"].replace("eth-mainnet", "base-mainnet")))
#         base_client.middleware_onion.inject(geth_poa_middleware, layer=0)


# if __name__ == '__main__':
#     lo()
