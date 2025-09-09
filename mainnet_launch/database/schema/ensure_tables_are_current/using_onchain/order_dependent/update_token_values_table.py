import pandas as pd
from multicall import Call
from web3 import Web3


from mainnet_launch.database.schema.full import (
    Tokens,
    TokenValues,
    AutopoolStates,
    DestinationStates,
    AutopoolDestinations,
)


from mainnet_launch.abis import STATS_CALCULATOR_REGISTRY_ABI
from mainnet_launch.data_fetching.get_events import fetch_events


from mainnet_launch.database.schema.postgres_operations import (
    get_full_table_as_orm,
    insert_avoid_conflicts,
    get_subset_not_already_in_column,
    merge_tables_as_df,
    TableSelector,
)
from mainnet_launch.data_fetching.get_state_by_block import (
    get_raw_state_by_blocks,
    safe_normalize_with_bool_success,
    identity_with_bool_success,
    get_state_by_one_block,
    make_dummy_1_call,
    safe_normalize_6_with_bool_success,
)
from mainnet_launch.constants import (
    ALL_CHAINS,
    ROOT_PRICE_ORACLE,
    ChainData,
    STATS_CALCULATOR_REGISTRY,
    WETH,
    SONIC_CHAIN,
    USDC,
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
        TokenValues,
        TokenValues.block,
        blocks_expected_to_have,
        where_clause=TokenValues.chain_id == chain.chain_id,
    )
    return blocks_to_fetch


def _fetch_new_token_values_rows(blocks: list[int], tokens_orms: list[Tokens], chain: ChainData) -> list[TokenValues]:
    """Returns the token safe price in USDC, and WETH for each to token in tokens_orms, and each block in blocks"""

    df = _fetch_safe_and_backing_values(blocks, tokens_orms, chain)

    new_token_values_rows = []

    def _extract_token_values_by_row(row: dict):
        for token in tokens_orms:
            for denominated_in in [WETH(chain), USDC(chain)]:

                backing = row.get((token.token_address, "backing"))
                backing = None if pd.isna(backing) else float(backing)

                safe_price = row[(token.token_address, denominated_in, "safe_price")]
                safe_price = None if pd.isna(safe_price) else float(safe_price)

                new_token_values_row = TokenValues(
                    block=int(row["block"]),
                    chain_id=chain.chain_id,
                    token_address=token.token_address,
                    denominated_in=denominated_in,
                    backing=backing,
                    safe_price=safe_price,
                )

                new_token_values_rows.append(new_token_values_row)

    df.apply(_extract_token_values_by_row, axis=1)
    return new_token_values_rows


def _fetch_and_insert_new_token_values(autopools: list[AutopoolConstants], chain: ChainData):
    needed_blocks = _determine_what_blocks_are_needed(autopools, chain)

    if not needed_blocks:
        return

    tokens_orms: list[Tokens] = get_full_table_as_orm(Tokens, where_clause=Tokens.chain_id == chain.chain_id)
    new_token_values_rows = _fetch_new_token_values_rows(needed_blocks, tokens_orms, chain)

    insert_avoid_conflicts(
        new_token_values_rows,
        TokenValues,
        index_elements=[
            TokenValues.block,
            TokenValues.chain_id,
            TokenValues.token_address,
            TokenValues.denominated_in,
        ],
    )


def _build_safe_price_calls(tokens: list[Tokens], chain: ChainData) -> list[Call]:

    eth_safe_price_calls = [
        Call(
            ROOT_PRICE_ORACLE(chain),
            ["getPriceInQuote(address,address)(uint256)", t.token_address, WETH(chain)],
            [((t.token_address, WETH(chain), "safe_price"), safe_normalize_with_bool_success)],
        )
        for t in tokens
    ]

    usdc_safe_price_calls = [
        Call(
            ROOT_PRICE_ORACLE(chain),
            ["getPriceInQuote(address,address)(uint256)", t.token_address, USDC(chain)],
            [((t.token_address, USDC(chain), "safe_price"), safe_normalize_6_with_bool_success)],
        )
        for t in tokens
    ]

    return [*eth_safe_price_calls, *usdc_safe_price_calls]


def _build_backing_calls(tokens: list[Tokens], chain: ChainData) -> list[Call]:
    # this is a self contained problem to make this more readable,
    # consider hardcoding it

    if chain == SONIC_CHAIN:
        # there are no calculators on sonic
        return []

    stats_calculator_registry_contract = chain.client.eth.contract(
        STATS_CALCULATOR_REGISTRY(chain), abi=STATS_CALCULATOR_REGISTRY_ABI
    )

    StatCalculatorRegistered = fetch_events(stats_calculator_registry_contract.events.StatCalculatorRegistered, chain)

    lstTokenAddress_calls = [
        Call(
            a,
            ["lstTokenAddress()(address)"],
            [(a, identity_with_bool_success)],
        )
        for a in StatCalculatorRegistered["calculatorAddress"]
    ]

    calculator_to_lst_address = get_state_by_one_block(
        lstTokenAddress_calls, int(max(StatCalculatorRegistered["block"])), chain=chain
    )
    StatCalculatorRegistered["lst"] = StatCalculatorRegistered["calculatorAddress"].map(calculator_to_lst_address)
    lst_calcs = StatCalculatorRegistered[~StatCalculatorRegistered["lst"].isna()].copy()
    # manual
    stETH = "0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84"  # stETH is rebasing to the backing is 1:1
    OETH = "0x856c4Efb76C1D1AE02e20CEB03A2A6a08b0b8dC3"  # OETH is rebasing, to the backing is 1:1

    lsts_to_exclude = [
        stETH,
        OETH,
        WETH(chain),
    ]
    backing_calls = [
        Call(
            calculatorAddress,
            ["calculateEthPerToken()(uint256)"],
            [((Web3.toChecksumAddress(token_address), "backing"), safe_normalize_with_bool_success)],
        )
        for (calculatorAddress, token_address) in zip(lst_calcs["calculatorAddress"], lst_calcs["lst"])
        if token_address.lower() not in lsts_to_exclude
    ]

    for lst in lsts_to_exclude:
        backing_calls.append(make_dummy_1_call((lst, "backing")))

    return backing_calls


def _fetch_safe_and_backing_values(missing_blocks: list[int], tokens: list[Tokens], chain: ChainData) -> pd.DataFrame:
    calls = [*_build_safe_price_calls(tokens, chain), *_build_backing_calls(tokens, chain)]

    # state = get_state_by_one_block(calls, max(missing_blocks), chain)
    df = get_raw_state_by_blocks(calls, missing_blocks, chain, include_block_number=True)
    return df


def ensure_token_values_are_current():
    # todo similar patterns, switch to threads as completed and per autopool, refactor to early exit and just do
    # at a per autopool level
    for chain in ALL_CHAINS:
        autopools = [a for a in ALL_AUTOPOOLS_DATA_ON_CHAIN if a.chain == chain]
        if autopools:
            _fetch_and_insert_new_token_values(autopools, chain)

        autopools = [a for a in ALL_AUTOPOOLS_DATA_FROM_REBALANCE_PLAN if a.chain == chain]
        if autopools:
            _fetch_and_insert_new_token_values(autopools, chain)


if __name__ == "__main__":
    from mainnet_launch.constants import profile_function

    profile_function(ensure_token_values_are_current)
