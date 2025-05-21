from multicall import Call


from mainnet_launch.database.schema.full import (
    DestinationTokenValues,
    AutopoolDestinationStates,
    DestinationStates,
    AutopoolDestinations,
    Destinations,
)
from mainnet_launch.database.schema.postgres_operations import (
    insert_avoid_conflicts,
    get_subset_not_already_in_column,
    merge_tables_as_df,
    TableSelector,
)
from mainnet_launch.data_fetching.get_state_by_block import (
    get_raw_state_by_blocks,
    safe_normalize_with_bool_success,
    safe_normalize_6_with_bool_success,
)
from mainnet_launch.constants import (
    ALL_CHAINS,
    ChainData,
    ALL_AUTOPOOLS_DATA_FROM_REBALANCE_PLAN,
    ALL_AUTOPOOLS_DATA_ON_CHAIN,
    AutopoolConstants,
)


def _determine_what_blocks_are_needed(autopools: list[AutopoolConstants], chain: ChainData) -> list[int]:
    blocks_expected_to_have = merge_tables_as_df(
        selectors=[
            TableSelector(
                AutopoolDestinations,
                [
                    AutopoolDestinations.destination_vault_address,
                    AutopoolDestinations.autopool_vault_address,
                ],
            ),
            TableSelector(
                DestinationStates,
                DestinationStates.block,
                join_on=(DestinationStates.destination_vault_address == AutopoolDestinations.destination_vault_address),
            ),
        ],
        where_clause=(DestinationStates.chain_id == chain.chain_id)
        & (AutopoolDestinations.autopool_vault_address.in_([a.autopool_eth_addr for a in autopools])),
    )["block"].unique()

    blocks_to_fetch = get_subset_not_already_in_column(
        AutopoolDestinationStates,
        AutopoolDestinationStates.block,
        blocks_expected_to_have,
        where_clause=AutopoolDestinationStates.autopool_vault_address.in_([a.autopool_eth_addr for a in autopools]),
    )
    blocks_to_fetch = [int(b) for b in blocks_to_fetch]
    return blocks_to_fetch


def _fetch_and_insert_new_autopool_destination_states(autopools: list[AutopoolConstants], chain: ChainData):
    destination_info_df = merge_tables_as_df(
        selectors=[
            TableSelector(
                AutopoolDestinations,
                [
                    AutopoolDestinations.destination_vault_address,
                    AutopoolDestinations.autopool_vault_address,
                ],
            ),
            TableSelector(
                Destinations,
                [Destinations.destination_vault_decimals],
                join_on=AutopoolDestinations.destination_vault_address == Destinations.destination_vault_address,
            ),
        ],
        where_clause=(AutopoolDestinations.chain_id == chain.chain_id)
        & (AutopoolDestinations.autopool_vault_address.in_([a.autopool_eth_addr for a in autopools]))
        & (
            AutopoolDestinations.autopool_vault_address != AutopoolDestinations.destination_vault_address
        ),  # exclude idle
    )

    balance_of_calls = []

    for autopool_vault_address, destination_vault_address, decimals in zip(
        destination_info_df["autopool_vault_address"],
        destination_info_df["destination_vault_address"],
        destination_info_df["destination_vault_decimals"],
    ):
        if decimals == 18:
            cleaning_function = safe_normalize_with_bool_success
        elif decimals == 6:
            cleaning_function = safe_normalize_6_with_bool_success

        balance_of_calls.append(
            Call(
                destination_vault_address,
                ["balanceOf(address)(uint256)", autopool_vault_address],
                [((autopool_vault_address, destination_vault_address, "balanceOf"), cleaning_function)],
            )
        )

    missing_blocks = _determine_what_blocks_are_needed(autopools, chain)

    if len(missing_blocks) == 0:
        return

    autopool_destination_balance_of_df = get_raw_state_by_blocks(
        balance_of_calls, missing_blocks, chain, include_block_number=True
    )

    new_autopool_destination_states_rows = []

    def _extract_autopool_destination_vault_balance_of_block(row: dict):
        for k in row.keys():
            if k != "block":
                autopool_vault_address, destination_vault_address, _ = k
                owned_shares = float(row[k]) if row[k] is not None else 0.0

                new_autopool_destination_states_rows.append(
                    AutopoolDestinationStates(
                        destination_vault_address=destination_vault_address,
                        autopool_vault_address=autopool_vault_address,
                        block=int(row["block"]),
                        chain_id=chain.chain_id,
                        owned_shares=owned_shares,
                    )
                )

    autopool_destination_balance_of_df.apply(_extract_autopool_destination_vault_balance_of_block, axis=1)

    idle_autopool_destination_states = _build_idle_autopool_destination_states(missing_blocks, autopools, chain)

    insert_avoid_conflicts(
        [*new_autopool_destination_states_rows, *idle_autopool_destination_states],
        AutopoolDestinationStates,
        index_elements=[
            AutopoolDestinationStates.destination_vault_address,
            AutopoolDestinationStates.autopool_vault_address,
            AutopoolDestinationStates.block,
            AutopoolDestinationStates.chain_id,
        ],
    )


def _build_idle_autopool_destination_states(
    missing_blocks: list[int], autopools: list[AutopoolConstants], chain: ChainData
) -> list[AutopoolDestinationStates]:
    # this is not correct, some
    # this is silently failng,
    idle_destination_token_value_df = merge_tables_as_df(
        selectors=[
            TableSelector(
                DestinationTokenValues,
                [
                    DestinationTokenValues.destination_vault_address,
                    DestinationTokenValues.block,
                    DestinationTokenValues.quantity,
                ],
            ),
        ],
        where_clause=(DestinationTokenValues.chain_id == chain.chain_id)
        & (DestinationTokenValues.destination_vault_address.in_([a.autopool_eth_addr for a in autopools]))
        & (DestinationTokenValues.block.in_(missing_blocks)),
    )

    idle_autopool_destination_states = []

    def _extract_idle_autopool_destination_state(row: dict):
        idle_autopool_destination_states.append(
            AutopoolDestinationStates(
                destination_vault_address=row["destination_vault_address"],
                autopool_vault_address=row["destination_vault_address"],  # same because this is idle
                block=int(row["block"]),
                chain_id=chain.chain_id,
                owned_shares=float(row["quantity"]),
            )
        )

    idle_destination_token_value_df.apply(_extract_idle_autopool_destination_state, axis=1)

    if len(idle_autopool_destination_states) == 0:
        raise ValueError("should not be 0, should have early stopped earlier")
    return idle_autopool_destination_states


def ensure_autopool_destination_states_are_current():
    for chain in ALL_CHAINS:
        autopools = [a for a in ALL_AUTOPOOLS_DATA_ON_CHAIN if a.chain == chain]
        if autopools:
            _fetch_and_insert_new_autopool_destination_states(autopools, chain)

        autopools = [a for a in ALL_AUTOPOOLS_DATA_FROM_REBALANCE_PLAN if a.chain == chain]
        if autopools:
            _fetch_and_insert_new_autopool_destination_states(autopools, chain)


if __name__ == "__main__":
    ensure_autopool_destination_states_are_current()
    # _determine_what_blocks_are_needed(ALL_AUTOPOOLS_DATA_FROM_REBALANCE_PLAN, ALL_CHAINS[0])
