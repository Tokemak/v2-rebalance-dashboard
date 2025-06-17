from dataclasses import dataclass
from time import time
import os

from pathlib import Path

from dotenv import load_dotenv
from web3 import Web3
from web3.middleware import geth_poa_middleware

load_dotenv()

ROOT_DIR = Path(__file__).parent  # consider moving these to a setup file with the db initalization
SOLVER_REBALANCE_PLANS_DIR = ROOT_DIR / "data_fetching/rebalance_plans"
WORKING_DATA_DIR = ROOT_DIR / "working_data"
DB_DIR = ROOT_DIR / "database"
DB_FILE = DB_DIR / "autopool_dashboard.db"

os.makedirs(SOLVER_REBALANCE_PLANS_DIR, exist_ok=True)
os.makedirs(WORKING_DATA_DIR, exist_ok=True)


eth_client = Web3(Web3.HTTPProvider(os.environ["ALCHEMY_URL"]))

base_client = Web3(Web3.HTTPProvider(os.environ["ALCHEMY_URL"].replace("eth-mainnet", "base-mainnet")))
base_client.middleware_onion.inject(geth_poa_middleware, layer=0)

sonic_client = Web3(Web3.HTTPProvider(os.environ["ALCHEMY_URL"].replace("eth-mainnet", "sonic-mainnet")))
sonic_client.middleware_onion.inject(geth_poa_middleware, layer=0)


os.environ["GAS_LIMIT"] = "550000000"
# make sure the chain ids are loaded as properties
eth_client.eth._chain_id = lambda: 1
base_client.eth._chain_id = lambda: 8453
sonic_client.eth._chain_id = lambda: 146


WEB3_CLIENTS: dict[str, Web3] = {
    "eth": eth_client,
    "base": base_client,
    "sonic": sonic_client,
}


@dataclass(frozen=True)
class ChainData:
    name: str
    block_autopool_first_deployed: int
    approx_seconds_per_block: float
    chain_id: int
    start_unix_timestamp: int

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
    symbol: str
    autopool_eth_addr: str
    autopool_eth_strategy_addr: str
    solver_rebalance_plans_bucket: str
    chain: ChainData
    base_asset: str  # AutopoolETH.asset()
    block_deployed: int
    data_from_rebalance_plan: bool
    base_asset_symbol: str
    start_display_date: str
    base_asset_decimals: int

    def __hash__(self):
        return hash(self.autopool_eth_addr)


ETH_CHAIN: ChainData = ChainData(
    name="eth",
    block_autopool_first_deployed=20722908,
    approx_seconds_per_block=12.0,
    chain_id=1,
    start_unix_timestamp=1726365887,
)


BASE_CHAIN: ChainData = ChainData(
    name="base",
    block_autopool_first_deployed=21241103,
    approx_seconds_per_block=2.0,
    chain_id=8453,
    start_unix_timestamp=1730591553,
)


SONIC_CHAIN: ChainData = ChainData(
    name="sonic",
    block_autopool_first_deployed=31593624,
    approx_seconds_per_block=0.5,  # TODO remove this arg
    chain_id=146,
    start_unix_timestamp=1748961926,
)

ALL_CHAINS = [ETH_CHAIN, BASE_CHAIN, SONIC_CHAIN]


@dataclass
class TokemakAddress:
    """For contracts that exist both on Ethereum and Base"""

    eth: str
    base: str
    sonic: str

    def __post_init__(self):

        for addr in [self.eth, self.base, self.sonic]:
            if not Web3.isChecksumAddress(addr):
                raise ValueError(f"{addr} must be a checksum address should be {Web3.toChecksumAddress(addr)=}")

    def __call__(self, chain: ChainData) -> str:
        """
        Returns the checksum address for this canonical address (eg USDC, WETH, SYSTEM_REGISTRY)
        """
        return getattr(self, chain.name)

    def __contains__(self, addr: str) -> bool:
        """
        Allows membership testing:
            addr in tokemak_address
        will be True if addr matches either self.eth or self.base (after checksumming).
        """
        try:
            check_sum_address = Web3.toChecksumAddress(addr)
        except Exception:
            return False
        return check_sum_address in [self.eth, self.base, self.sonic]


DEAD_ADDRESS = "0x000000000000000000000000000000000000dEaD"


SYSTEM_REGISTRY = TokemakAddress(
    eth="0x2218F90A98b0C070676f249EF44834686dAa4285",
    base="0x18Dc926095A7A007C01Ef836683Fdef4c4371b4e",
    sonic="0x1a912EB51D3cF8364eBAEE5A982cA37f25aD8848",
)

AUTOPOOL_REGISTRY = TokemakAddress(
    eth="0x7E5828a3A6Ae75426d739E798140513A2E2964E4",
    base="0x4fE7916A10B15DADEFc59D06AC81757112b1feCE",
    sonic="0x63E8e5aeBcC8C77BD4411aba375FcBDd9ce8C253",
)

ROOT_PRICE_ORACLE = TokemakAddress(
    eth="0x61F8BE7FD721e80C0249829eaE6f0DAf21bc2CaC",
    base="0xBCf67d1d643C53E9C2f84aCBd830A5EDC2661795",
    sonic="0x356d6e38efd2f33B162eC63534B449B96846751F",
)

LENS_CONTRACT = TokemakAddress(
    eth="0x146b5564dd061D648275e4Bd3569b8c285783882",
    base="0xaF05c205444c5884F53492500Bed22A8f617Aa9C",
    sonic="0xCB7E450c32D21Eb0168466c8022Ae32EF785a163",
)

DESTINATION_VAULT_REGISTRY = TokemakAddress(
    eth="0x3AaC1CE01127593CA0c7f87b1Aedb1E153e152aE",
    base="0xBBBB6E844EEd5952B44C2063670093E27E21735f",
    sonic="0x005B5DD2182F4ADf9fCA299e762029337FF79fA8",
)

INCENTIVE_PRICING_STATS = TokemakAddress(
    eth="0x8607bA6540AF378cbA64F4E3497FBb2d1385f862",
    base="0xF28213d5cbc9f4cfB371599D25E232978848090d",
    sonic=DEAD_ADDRESS,
)

LIQUIDATION_ROW = TokemakAddress(
    eth="0xBf58810BB1946429830C1f12205331608c470ff5",
    base="0xE2F00bbC3E5ddeCfBD95e618CE36b49F38881d4f",
    sonic="0xf3b137219325466004AEb91CAa0A0Bdd2A8afc8e",
)

# only autoLRT on mainnet uses points
POINTS_HOOK = TokemakAddress(
    eth="0xA386067eB5F7Dc9b731fe1130745b0FB00c615C3",
    base=DEAD_ADDRESS,
    sonic=DEAD_ADDRESS,
)

STATS_CALCULATOR_REGISTRY = TokemakAddress(
    eth="0xaE6b250841fA7520AF843c776aA58E23060E2124",
    base="0x22dd2189728B40409476F4F80CA8f2f6BdB217D2",
    sonic=DEAD_ADDRESS,
)

WETH = TokemakAddress(
    eth="0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
    base="0x4200000000000000000000000000000000000006",
    sonic="0x50c42dEAcD8Fc9773493ED674b675bE577f2634b",
)

USDC = TokemakAddress(
    eth="0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
    base="0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
    sonic="0x29219dd400f2Bf60E5a23d13Be72B486D4038894",
)

DOLA = TokemakAddress(
    eth="0x865377367054516e17014CcdED1e7d814EDC9ce4",
    base="0x4621b7A9c75199271F773Ebd9A499dbd165c3191",
    sonic=DEAD_ADDRESS,
)


def time_decorator(func):
    def wrapper(*args, **kwargs):
        start_time = time()
        result = func(*args, **kwargs)
        elapsed_time = time() - start_time
        print(f"{func.__name__} took {elapsed_time:.4f} seconds.")
        return result

    return wrapper


PRODUCTION_LOG_FILE_NAME = "production_usage.log"
TEST_LOG_FILE_NAME = "test_pages.log"
STARTUP_LOG_FILE = ROOT_DIR / "startup.csv"


AUTO_ETH: AutopoolConstants = AutopoolConstants(
    "autoETH",
    "autoETH",
    autopool_eth_addr="0x0A2b94F6871c1D7A32Fe58E1ab5e6deA2f114E56",
    autopool_eth_strategy_addr="0xf5f6addB08c5e6091e5FdEc7326B21bEEd942235",
    solver_rebalance_plans_bucket=os.environ["AUTO_ETH_BUCKET"],
    chain=ETH_CHAIN,
    base_asset=WETH(ETH_CHAIN),
    block_deployed=20722908,
    data_from_rebalance_plan=False,
    base_asset_symbol="ETH",
    start_display_date="9-14-2024",
    base_asset_decimals=18,
)

BAL_ETH: AutopoolConstants = AutopoolConstants(
    "balETH",
    "balETH",
    autopool_eth_addr="0x6dC3ce9C57b20131347FDc9089D740DAf6eB34c5",
    autopool_eth_strategy_addr="0xabe104560D0B390309bcF20b73Dca335457AA32e",
    solver_rebalance_plans_bucket=os.environ["BAL_ETH_BUCKET"],
    chain=ETH_CHAIN,
    base_asset=WETH(ETH_CHAIN),
    block_deployed=20722909,
    data_from_rebalance_plan=False,
    base_asset_symbol="ETH",
    start_display_date="9-14-2024",
    base_asset_decimals=18,
)

AUTO_LRT: AutopoolConstants = AutopoolConstants(
    "autoLRT",
    "autoLRT",
    autopool_eth_addr="0xE800e3760FC20aA98c5df6A9816147f190455AF3",
    autopool_eth_strategy_addr="0x72a726c10220280049687E58B7b05fb03d579109",
    solver_rebalance_plans_bucket=os.environ["AUTO_LRT_BUCKET"],
    chain=ETH_CHAIN,
    base_asset=WETH(ETH_CHAIN),
    block_deployed=20722910,
    data_from_rebalance_plan=False,
    base_asset_symbol="ETH",
    start_display_date="9-14-2024",
    base_asset_decimals=18,
)

BASE_ETH: AutopoolConstants = AutopoolConstants(
    "baseETH",
    "baseETH",
    autopool_eth_addr="0xAADf01DD90aE0A6Bb9Eb908294658037096E0404",
    autopool_eth_strategy_addr="0xe72a466d426F735BfeE91Db19dc509735B65b8dc",
    solver_rebalance_plans_bucket=os.environ["BASE_ETH_BUCKET"],
    chain=BASE_CHAIN,
    base_asset=WETH(BASE_CHAIN),
    block_deployed=21241103,
    data_from_rebalance_plan=False,
    base_asset_symbol="ETH",
    start_display_date="11-5-2024",
    base_asset_decimals=18,
)

DINERO_ETH: AutopoolConstants = AutopoolConstants(
    "dineroETH",
    "dineroETH",
    autopool_eth_addr="0x35911af1B570E26f668905595dEd133D01CD3E5a",
    autopool_eth_strategy_addr="0x2Ade538C621A117afc4D485C79b16DD5769bC921",
    solver_rebalance_plans_bucket=os.environ["DINERO_ETH_BUCKET"],
    chain=ETH_CHAIN,
    base_asset=WETH(ETH_CHAIN),
    block_deployed=21718586,
    data_from_rebalance_plan=False,
    base_asset_symbol="ETH",
    start_display_date="2-9-2025",
    base_asset_decimals=18,
)

AUTO_USD: AutopoolConstants = AutopoolConstants(
    "autoUSD",
    "autoUSD",
    autopool_eth_addr="0xa7569A44f348d3D70d8ad5889e50F78E33d80D35",
    autopool_eth_strategy_addr=None,
    solver_rebalance_plans_bucket=os.environ["AUTO_USD_BUCKET"],
    chain=ETH_CHAIN,
    base_asset=USDC(ETH_CHAIN),
    block_deployed=22032640,
    data_from_rebalance_plan=True,
    base_asset_symbol="USDC",
    start_display_date="4-8-2025",
    base_asset_decimals=6,
)


BASE_USD: AutopoolConstants = AutopoolConstants(
    "baseUSD",
    "baseUSD",
    autopool_eth_addr="0x9c6864105AEC23388C89600046213a44C384c831",
    autopool_eth_strategy_addr=None,
    solver_rebalance_plans_bucket=os.environ["BASE_USD_BUCKET"],
    chain=BASE_CHAIN,
    base_asset=USDC(BASE_CHAIN),
    block_deployed=30310652,
    data_from_rebalance_plan=True,
    base_asset_symbol="USDC",
    start_display_date="5-16-2025",
    base_asset_decimals=6,
)


AUTO_DOLA: AutopoolConstants = AutopoolConstants(
    "autoDOLA",
    "autoDOLA",
    autopool_eth_addr="0x79eB84B5E30Ef2481c8f00fD0Aa7aAd6Ac0AA54d",
    autopool_eth_strategy_addr=None,
    solver_rebalance_plans_bucket=os.environ["AUTO_DOLA_BUCKET"],
    chain=ETH_CHAIN,
    base_asset=DOLA(ETH_CHAIN),
    block_deployed=22582955,
    data_from_rebalance_plan=True,
    base_asset_symbol="DOLA",
    start_display_date="5-28-2025",
    base_asset_decimals=18,
)


SONIC_USD: AutopoolConstants = AutopoolConstants(
    "sonicUSD",
    "sonicUSD",
    autopool_eth_addr="0xCb119265AA1195ea363D7A243aD56c73EA42Eb59",
    autopool_eth_strategy_addr=None,
    solver_rebalance_plans_bucket=os.environ["SONIC_USD_BUCKET"],
    chain=SONIC_CHAIN,
    base_asset=USDC(SONIC_CHAIN),
    block_deployed=31593624,
    data_from_rebalance_plan=True,
    base_asset_symbol="USDC",
    start_display_date="6-03-2025",  # TODO edit this date
    base_asset_decimals=6,
)


ALL_AUTOPOOLS: list[AutopoolConstants] = [
    AUTO_ETH,
    BAL_ETH,
    AUTO_LRT,
    BASE_ETH,
    DINERO_ETH,
    AUTO_USD,
    BASE_USD,
    AUTO_DOLA,
    SONIC_USD,
]

ALL_AUTOPOOLS_DATA_ON_CHAIN: list[AutopoolConstants] = [AUTO_ETH, BAL_ETH, AUTO_LRT, BASE_ETH, DINERO_ETH]
ALL_AUTOPOOLS_DATA_FROM_REBALANCE_PLAN: list[AutopoolConstants] = [AUTO_USD, BASE_USD, AUTO_DOLA, SONIC_USD]
