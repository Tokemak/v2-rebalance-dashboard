import time

from web3 import Web3
from web3.middleware import geth_poa_middleware

from .secrets import ALCHEMY_URL, TOKEMAK_SUBGRAPH_URLS
from .models import ChainData


eth_client = Web3(Web3.HTTPProvider(ALCHEMY_URL))

base_client = Web3(Web3.HTTPProvider(ALCHEMY_URL.replace("eth-mainnet", "base-mainnet")))
base_client.middleware_onion.inject(geth_poa_middleware, layer=0)

sonic_client = Web3(Web3.HTTPProvider(ALCHEMY_URL.replace("eth-mainnet", "sonic-mainnet")))
sonic_client.middleware_onion.inject(geth_poa_middleware, layer=0)

arbitrum_client = Web3(Web3.HTTPProvider(ALCHEMY_URL.replace("eth-mainnet", "arb-mainnet")))

eth_client.eth._chain_id = lambda: 1
base_client.eth._chain_id = lambda: 8453
sonic_client.eth._chain_id = lambda: 146
arbitrum_client.eth._chain_id = lambda: 42161

WEB3_CLIENTS: dict[str, Web3] = {
    "eth": eth_client,
    "base": base_client,
    "sonic": sonic_client,
    "arb": arbitrum_client,
}


def _add_retry_get_block_number(
    client: Web3,
    retries: int = 3,
    backoff: float = 1.0,
) -> None:
    """
    Monkey-patch client.eth.get_block_number to retry on failure.
    retries: total attempts (including the first)
    backoff: initial sleep in seconds; doubles each retry
    """
    original_fn = client.eth.get_block_number

    def get_block_number_with_retry() -> int:
        delay = backoff
        for attempt in range(1, retries + 1):
            try:
                return original_fn()
            except Exception:
                if attempt == retries:
                    raise
                time.sleep(delay)
                delay *= 2

    client.eth.get_block_number = get_block_number_with_retry


for client in WEB3_CLIENTS.values():
    _add_retry_get_block_number(client, retries=4, backoff=0.5)


ETH_CHAIN = ChainData(
    name="eth",
    block_autopool_first_deployed=20722908,
    chain_id=1,
    start_unix_timestamp=1726365887,
    tokemak_subgraph_url=TOKEMAK_SUBGRAPH_URLS["eth"],
    alchemy_network_enum="eth-mainnet",
)


BASE_CHAIN = ChainData(
    name="base",
    block_autopool_first_deployed=21241103,
    chain_id=8453,
    start_unix_timestamp=1730591553,
    tokemak_subgraph_url=TOKEMAK_SUBGRAPH_URLS["base"],
    alchemy_network_enum="base-mainnet",
)


SONIC_CHAIN = ChainData(
    name="sonic",
    block_autopool_first_deployed=31593624,
    chain_id=146,
    start_unix_timestamp=1748961926,
    tokemak_subgraph_url=TOKEMAK_SUBGRAPH_URLS["sonic"],
    alchemy_network_enum="sonic-mainnet",
)


ARBITRUM_CHAIN = ChainData(
    name="arb",
    block_autopool_first_deployed=377406050,
    chain_id=42161,
    start_unix_timestamp=1757439586,
    tokemak_subgraph_url=TOKEMAK_SUBGRAPH_URLS["arb"],
    alchemy_network_enum="arb-mainnet",
)


ALL_CHAINS = [ETH_CHAIN, BASE_CHAIN, SONIC_CHAIN, ARBITRUM_CHAIN]
