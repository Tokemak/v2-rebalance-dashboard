from dataclasses import dataclass
from functools import cached_property

from typing import cast
import importlib

from web3 import Web3


@dataclass(frozen=True)
class ChainData:
    name: str
    block_autopool_first_deployed: int
    chain_id: int
    start_unix_timestamp: int
    tokemak_subgraph_url: str

    def __hash__(self):
        return self.chain_id

    @cached_property
    def client(self) -> "Web3":
        """
        Dynamically retrieves the Web3 client associated with this chain.

        This is required to ensure that ChainData is hashable so can be used in

        @st.cache_data(ttl=CACHE_TIME)

        You may want to optimize this, or refactor it down the line
        """
        chains = importlib.import_module("mainnet_launch.constants.chains")
        clients = getattr(chains, "WEB3_CLIENTS", None)
        if clients is None or self.name not in clients:
            raise ValueError(f"No Web3 client configured for chain: {self.name}")
        return cast("Web3", clients[self.name])


@dataclass(frozen=True)
class AutopoolConstants:
    name: str
    symbol: str
    autopool_eth_addr: str
    autopool_eth_strategy_addr: str | None
    solver_rebalance_plans_bucket: str | None
    chain: ChainData
    base_asset: str
    block_deployed: int
    data_from_rebalance_plan: bool
    base_asset_symbol: str
    start_display_date: str
    base_asset_decimals: int


@dataclass
class TokemakAddress:
    eth: str
    base: str
    sonic: str
    name: str

    def __post_init__(self):
        for addr in [self.eth, self.base, self.sonic]:
            if not Web3.isChecksumAddress(addr):
                raise ValueError(f"{addr} must be a checksum address should be {Web3.toChecksumAddress(addr)=}")

    def __call__(self, chain: ChainData) -> str:
        """
        Returns the checksum address for this (thing) on the specified chain.
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

    def __hash__(self):
        """
        Hashes the address based on the Ethereum address.
        This is useful for caching and ensuring uniqueness.
        """
        return hash(self.eth + self.base + self.sonic)
