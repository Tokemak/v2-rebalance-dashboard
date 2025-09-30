from multicall import Call

from mainnet_launch.constants import ChainData, ALL_AUTOPOOLS

from mainnet_launch.data_fetching.get_state_by_block import (
    get_state_by_one_block,
    identity_with_bool_success,
    to_checksum_address_with_bool_success,
)
from mainnet_launch.database.schema.ensure_tables_are_current.using_onchain.helpers.update_blocks import (
    ensure_all_blocks_are_in_table,
)

from mainnet_launch.database.schema.full import Autopools
from mainnet_launch.database.postgres_operations import (
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
                Call(v, "autoPoolStrategy()(address)", [((v, "strategy"), to_checksum_address_with_bool_success)]),
                Call(v, "asset()(address)", [((v, "asset"), to_checksum_address_with_bool_success)]),
            ]
        )

    return get_state_by_one_block(calls, block=chain.get_block_near_top(), chain=chain)


def ensure_autopools_are_current() -> None:
    """
    Make sure that the Destinations, DestinationTokens and Tokens tables are current for all the underlying tokens in each of the destinations
    """

    autopool_vault_addresses = [a.autopool_eth_addr for a in ALL_AUTOPOOLS]
    autopools_not_in_table = get_subset_not_already_in_column(
        Autopools,
        Autopools.autopool_vault_address,
        autopool_vault_addresses,
    )
    if not autopools_not_in_table:
        return

    for autopool in ALL_AUTOPOOLS:
        if autopool.autopool_eth_addr in autopools_not_in_table:
            autopool_state_dict = _fetch_autopool_state_dicts([autopool.autopool_eth_addr], autopool.chain)

            new_autopool_row = Autopools(
                autopool_vault_address=autopool.autopool_eth_addr,
                chain_id=autopool.chain.chain_id,
                block_deployed=autopool.block_deployed,
                name=autopool_state_dict[(autopool.autopool_eth_addr, "name")],
                symbol=autopool_state_dict[(autopool.autopool_eth_addr, "symbol")],
                strategy_address=autopool_state_dict[(autopool.autopool_eth_addr, "strategy")],
                base_asset=autopool_state_dict[(autopool.autopool_eth_addr, "asset")],
                data_from_rebalance_plan=autopool.data_from_rebalance_plan,
            )

            ensure_all_blocks_are_in_table([autopool.block_deployed], autopool.chain)

            insert_avoid_conflicts([new_autopool_row], Autopools)


if __name__ == "__main__":

    ensure_autopools_are_current()
    # from mainnet_launch.constants import profile_function

    # profile_function(ensure_autopools_are_current)
