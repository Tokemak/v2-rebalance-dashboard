from dataclasses import dataclass

from web3 import Web3


@dataclass(frozen=True)
class ChainData:
    name: str
    block_autopool_first_deployed: int
    approx_seconds_per_block: float  # TODO remove this arg
    chain_id: int
    start_unix_timestamp: int
    tokemak_subgraph_url: str


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

    def __hash__(self):
        """
        Hashes the address based on the Ethereum address.
        This is useful for caching and ensuring uniqueness.
        """
        return hash(self.eth + self.base + self.sonic)
