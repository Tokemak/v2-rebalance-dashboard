import os
from enum import Enum
from dataclasses import dataclass
from dotenv import load_dotenv
from web3 import Web3
from multicall import Call

load_dotenv()

AUTO_ETH_BUCKET = os.environ["AUTO_ETH_BUCKET"]
BAL_ETH_BUCKET = os.environ["BAL_ETH_BUCKET"]
AUTO_LRT_BUCKET = os.environ["AUTO_LRT_BUCKET"]
ALCHEMY_URL = os.environ["ALCHEMY_URL"]

eth_client = Web3(Web3.HTTPProvider(ALCHEMY_URL))
base_client = Web3(Web3.HTTPProvider(ALCHEMY_URL.replace("eth-mainnet", "base-mainnet")))


class Chain(Enum):
    ETH = "eth"
    BASE = "base"
    # can add more chains here later

    def client(self) -> Web3:
        """Returns the Web3 client associated with the chain."""
        if self == Chain.ETH:
            return eth_client
        elif self == Chain.BASE:
            return base_client
        else:
            raise ValueError(f"No client available for chain: {self.name}")


@dataclass
class TokemakAddress:
    eth: str
    base: str

    def __post_init__(self):
        if not Web3.isChecksumAddress(self.eth):
            raise ValueError(f"{self.eth=} must be a checksum address {Web3.toChecksumAddress(self.eth)=}")
        if not Web3.isChecksumAddress(self.base):
            raise ValueError(f"{self.base=} must be a checksum address {Web3.toChecksumAddress(self.base)=}")

    def __call__(self, chain: Chain) -> str:
        address = getattr(self, chain.value, None)
        if not address:
            raise ValueError(f"No address defined for chain: {chain.name}")
        return address


@dataclass
class AutopoolConstants:
    name: str
    autopool_eth_addr: str
    autopool_eth_strategy_addr: str
    solver_rebalance_plans_bucket: str
    chain: Chain


AUTO_ETH = AutopoolConstants(
    "autoETH",  #  "Tokemak autoETH",
    "0x0A2b94F6871c1D7A32Fe58E1ab5e6deA2f114E56",
    "0xf5f6addB08c5e6091e5FdEc7326B21bEEd942235",
    AUTO_ETH_BUCKET,
    chain=Chain.ETH,
)

# example usage


def safe_normalize_with_bool_success(success, data):
    if success:
        return data / 1e18


def totalSupply_call(name: str, token: TokemakAddress, chain: Chain) -> Call:
    return Call(
        token(chain),
        ["totalSupply()(uint256)"],
        [(name, safe_normalize_with_bool_success)],
    )


WETH = TokemakAddress(
    eth="0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2", base="0x4200000000000000000000000000000000000006"
)
