from multicall import Call
from web3 import Web3

from mainnet_launch.constants import ChainData, ALL_AUTOPOOLS, ALL_CHAINS

from mainnet_launch.data_fetching.get_state_by_block import get_state_by_one_block, identity_with_bool_success
from mainnet_launch.data_fetching.block_timestamp import ensure_all_blocks_are_in_table

from mainnet_launch.database.schema.full import Autopools
from mainnet_launch.database.schema.postgres_operations import (
    insert_avoid_conflicts,
    get_subset_not_already_in_column,
)


def _fetch_autopool_state_dicts(autopool_vault_addresses: list[str], chain: ChainData) -> dict[tuple[str, str], any]:
    calls = []
    for v in autopool_vault_addresses:
        calls.extend(
            [
                Call(v, "symbol()(string)", [((v, "symbol"), identity_with_bool_success)]),
                Call(v, "name()(string)", [((v, "name"), identity_with_bool_success)]),
                Call(v, "autoPoolStrategy()(address)", [((v, "strategy"), identity_with_bool_success)]),
                Call(v, "asset()(address)", [((v, "asset"), identity_with_bool_success)]),
            ]
        )
    return get_state_by_one_block(calls, block=chain.client.eth.block_number, chain=chain)  # just some current block


def ensure_autopools_are_current() -> None:
    """
    Make sure that the Destinations, DestinationTokens and Tokens tables are current for all the underlying tokens in each of the destinations
    """
    for chain in ALL_CHAINS:
        autopool_vault_addresses = [a.autopool_eth_addr for a in ALL_AUTOPOOLS if a.chain == chain]
        autopools_not_in_table = get_subset_not_already_in_column(
            Autopools,
            Autopools.autopool_vault_address,
            autopool_vault_addresses,
            where_clause=Autopools.chain_id == chain.chain_id,
        )
        if len(autopools_not_in_table) == 0:
            continue

        autopool_state_dict = _fetch_autopool_state_dicts(autopools_not_in_table, chain)
        autopools_to_add = [a for a in ALL_AUTOPOOLS if a.autopool_eth_addr in autopools_not_in_table]

        autopools_rows = [
            Autopools(
                autopool_vault_address=a.autopool_eth_addr,
                chain_id=a.chain.chain_id,
                block_deployed=a.block_deployed,
                name=autopool_state_dict[(a.autopool_eth_addr, "name")],
                symbol=autopool_state_dict[(a.autopool_eth_addr, "symbol")],
                strategy_address=Web3.toChecksumAddress(autopool_state_dict[(a.autopool_eth_addr, "strategy")]),
                base_asset=Web3.toChecksumAddress(autopool_state_dict[(a.autopool_eth_addr, "asset")]),
                data_from_rebalance_plan=a.data_from_rebalance_plan,
            )
            for a in autopools_to_add
        ]
        new_blocks = [a.block_deployed for a in autopools_rows]
        ensure_all_blocks_are_in_table(new_blocks, chain)
        insert_avoid_conflicts(
            autopools_rows, Autopools, index_elements=[Autopools.autopool_vault_address, Autopools.chain_id]
        )


if __name__ == "__main__":
    ensure_autopools_are_current()
