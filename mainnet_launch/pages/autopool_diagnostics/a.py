from mainnet_launch.constants.constants import DESTINATION_VAULT_REGISTRY, WETH, ETH_CHAIN, eth_client
from mainnet_launch.abis import DESTINATION_VAULT_REGISTRY_ABI
from mainnet_launch.data_fetching.get_events import fetch_events

contract = eth_client.eth.contract(DESTINATION_VAULT_REGISTRY(ETH_CHAIN), abi=DESTINATION_VAULT_REGISTRY_ABI)


DestinationVaultRegistered = fetch_events(
    contract.events.DestinationVaultRegistered, start_block=19_000_000, end_block=None, chain=ETH_CHAIN
)
DestinationVaultRegistered
