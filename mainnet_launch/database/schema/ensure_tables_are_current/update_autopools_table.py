from multicall import Call


from mainnet_launch.constants import ChainData, ALL_AUTOPOOLS, ALL_CHAINS

from mainnet_launch.data_fetching.get_state_by_block import get_state_by_one_block, identity_with_bool_success
from mainnet_launch.data_fetching.block_timestamp import ensure_all_blocks_are_in_table

from mainnet_launch.database.schema.full import Autopools
from mainnet_launch.database.schema.postgres_operations import (
    insert_avoid_conflicts,
    get_subset_not_already_in_column,
)


def _fetch_autopool_state_dicts(autopool_vault_addresses: list[str], chain: ChainData) -> list[dict]:
    symbol_calls = [
        Call(
            t,
            "symbol()(string)",
            [(t + "_symbol", identity_with_bool_success)],
        )
        for t in autopool_vault_addresses
    ]

    name_calls = [
        Call(
            t,
            "name()(string)",
            [(t + "_name", identity_with_bool_success)],
        )
        for t in autopool_vault_addresses
    ]

    autopool_strategy_calls = [
        Call(
            t,
            "autoPoolStrategy()(address)",
            [(t + "_strategy", identity_with_bool_success)],
        )
        for t in autopool_vault_addresses
    ]

    asset_calls = [
        Call(
            t,
            "asset()(address)",
            [(t + "_asset", identity_with_bool_success)],
        )
        for t in autopool_vault_addresses
    ]

    raw = get_state_by_one_block(
        [*symbol_calls, *name_calls, *autopool_strategy_calls, *asset_calls], chain.client.eth.block_number, chain
    )

    symbol_dict = {v: raw[f"{v}_symbol"] for v in autopool_vault_addresses}
    name_dict = {v: raw[f"{v}_name"] for v in autopool_vault_addresses}
    strategy_dict = {v: raw[f"{v}_strategy"] for v in autopool_vault_addresses}
    asset_dict = {v: raw[f"{v}_asset"] for v in autopool_vault_addresses}

    return symbol_dict, name_dict, strategy_dict, asset_dict


def ensure_autopools_is_current() -> None:
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

        symbol_dict, name_dict, strategy_dict, asset_dict = _fetch_autopool_state_dicts(autopools_not_in_table, chain)
        autopools_to_add = [a for a in ALL_AUTOPOOLS if a.autopool_eth_addr in autopools_not_in_table]

        autopools_rows = [
            Autopools(
                autopool_vault_address=a.autopool_eth_addr,
                chain_id=a.chain.chain_id,
                block_deployed=a.block_deployed,
                name=name_dict[a.autopool_eth_addr],
                symbol=symbol_dict[a.autopool_eth_addr],
                strategy_address=strategy_dict[a.autopool_eth_addr],
                base_asset=asset_dict[a.autopool_eth_addr],
            )
            for a in autopools_to_add
        ]
        new_blocks = [a.block_deployed for a in autopools_rows]
        ensure_all_blocks_are_in_table(new_blocks, chain)
        insert_avoid_conflicts(
            autopools_rows, Autopools, index_elements=[Autopools.autopool_vault_address, Autopools.chain_id]
        )


if __name__ == "__main__":
    ensure_autopools_is_current()
