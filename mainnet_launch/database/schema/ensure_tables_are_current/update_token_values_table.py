import pandas as pd
from multicall import Call
import numpy as np
from web3 import Web3


from mainnet_launch.database.schema.full import (
    DestinationStates,
    DestinationTokenValues,
    AutopoolDestinationStates,
    Autopools,
    DestinationTokens,
    Destinations,
    Tokens,
    TokenValues,
)


from mainnet_launch.abis import STATS_CALCULATOR_REGISTRY_ABI
from mainnet_launch.data_fetching.get_events import fetch_events


from mainnet_launch.database.schema.postgres_operations import (
    get_full_table_as_df,
    get_full_table_as_orm,
    insert_avoid_conflicts,
    get_highest_value_in_field_where,
    get_subset_not_already_in_column,
)
from mainnet_launch.data_fetching.get_state_by_block import (
    get_raw_state_by_blocks,
    safe_normalize_with_bool_success,
    safe_normalize_6_with_bool_success,
    build_blocks_to_use,
    identity_with_bool_success,
    get_state_by_one_block,
)
from mainnet_launch.pages.autopool_diagnostics.lens_contract import (
    fetch_active_destinations_by_autopool_by_block,
    fetch_pools_and_destinations_df,
)
from mainnet_launch.constants import ROOT_PRICE_ORACLE, ChainData, STATS_CALCULATOR_REGISTRY, WETH


def ensure_token_values_are_current(chain: ChainData):
    possible_blocks = build_blocks_to_use(chain)

    missing_blocks = get_subset_not_already_in_column(
        TokenValues,
        TokenValues.block,
        possible_blocks,
        where_clause=TokenValues.chain_id == chain.chain_id,
    )

    all_tokens_orm: list[Tokens] = get_full_table_as_orm(Tokens, where_clause=Tokens.chain_id == chain.chain_id)

    df = _fetch_safe_and_backing_values(missing_blocks, all_tokens_orm, chain)

    new_token_values_rows = []

    def _extract_token_values_by_row(row: dict):
        for token in all_tokens_orm:
            backing = row.get(f"{token.address}_backing")
            backing = None if pd.isna(backing) else float(backing)

            safe_price = row[f"{token.address}_safe"]
            safe_price = None if pd.isna(safe_price) else float(safe_price)
            if token.address == WETH(chain):
                pass

            new_token_values_row = TokenValues(
                block=int(row["block"]),
                chain_id=chain.chain_id,
                token_address=token.address,
                denomiated_in=WETH(chain),
                backing=backing,
                safe_price=safe_price,
            )
            new_token_values_rows.append(new_token_values_row)

    df.apply(_extract_token_values_by_row, axis=1)

    return 
    insert_avoid_conflicts(
        new_token_values_rows,
        TokenValues,
        index_elements=[TokenValues.block, TokenValues.chain_id, TokenValues.token_address],
    )


def _build_safe_price_calls(tokens: list[Tokens], chain: ChainData) -> pd.DataFrame:
    safe_price_eth_calls = [
        Call(
            ROOT_PRICE_ORACLE(chain),
            ["getPriceInEth(address)(uint256)", t.address],
            [(t.address + "_safe", safe_normalize_with_bool_success)],
        )
        for t in tokens
    ]

    # TODO add USDC safe price here later
    # get Price in quote, (USDC) see stable coin branch

    return safe_price_eth_calls


def _build_backing_calls(tokens: list[Tokens], chain: ChainData) -> list[Call]:
    # this is a self contained problem to make this more readable,
    # my inclanation is to hard code it there are < 10 LSTs and stablecoins

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

    backing_calls = [
        Call(
            calculatorAddress,
            ["calculateEthPerToken()(uint256)"],
            [(f"{token_address}_backing", safe_normalize_with_bool_success)],
        )
        for (calculatorAddress, token_address) in zip(lst_calcs["calculatorAddress"], lst_calcs["lst"])
    ]

    dummy_weth_backing_call = make_dummy_1_call(WETH(chain) + "_backing")
    # TODO ADD ETH HERE too eeeeeeeeeeeeeeeEEEEEEEeeee version (approx)
    backing_calls.append(dummy_weth_backing_call)
    return backing_calls


def _constant_1(success, value) -> float:
    return 1.0


def make_dummy_1_call(name: str) -> Call:
    return Call(
        "0x000000000000000000000000000000000000dEaD",
        ["dummy()(uint256)"],
        [(name, _constant_1)],
    )


def _fetch_safe_and_backing_values(missing_blocks: list[int], tokens: list[Tokens], chain: ChainData) -> pd.DataFrame:
    calls = [*_build_safe_price_calls(tokens, chain), *_build_backing_calls(tokens, chain)]
    df = get_raw_state_by_blocks(calls, missing_blocks, chain, include_block_number=True)
    return df


if __name__ == "__main__":
    from mainnet_launch.constants import ETH_CHAIN

    ensure_token_values_are_current(ETH_CHAIN)


# #class TokenValues(Base):
#     __tablename__ = "token_values"

#     block: Mapped[int] = mapped_column(primary_key=True)
#     chain_id: Mapped[int] = mapped_column(primary_key=True)
#     token_address: Mapped[str] = mapped_column(primary_key=True)

#     denomiated_in: Mapped[str] = mapped_column(nullable=False)
#     backing: Mapped[float] = mapped_column(nullable=True)
#     safe_price: Mapped[float] = mapped_column(nullable=True)

#     __table_args__ = (
#         # link back to blocks
#         ForeignKeyConstraint(["block", "chain_id"], ["blocks.block", "blocks.chain_id"]),
#         # composite FK into tokens(address, chain_id)
#         ForeignKeyConstraint(["token_address", "chain_id"], ["tokens.address", "tokens.chain_id"]),
#     )

# class DestinationTokenValues(Base):
#     __tablename__ = "destination_token_values"

#     block: Mapped[int] = mapped_column(primary_key=True)
#     chain_id: Mapped[int] = mapped_column(primary_key=True)
#     token_address: Mapped[str] = mapped_column(nullable=False)
#     destination_address: Mapped[str] = mapped_column(nullable=False)

#     spot_price: Mapped[float] = mapped_column(nullable=True)
#     quantity: Mapped[float] = mapped_column(nullable=False)
#     safe_spot_spread: Mapped[float] = mapped_column(nullable=True)
#     spot_backing_discount: Mapped[float] = mapped_column(nullable=True)

#     __table_args__ = (
#         # point (block, chain_id) → blocks
#         ForeignKeyConstraint(["block", "chain_id"], ["blocks.block", "blocks.chain_id"]),
#         # composite FK into tokens(address, chain_id)
#         ForeignKeyConstraint(["token_address", "chain_id"], ["tokens.address", "tokens.chain_id"]),
#         # composite FK into destinations(destination_vault_address, chain_id)
#         ForeignKeyConstraint(
#             ["destination_address", "chain_id"], ["destinations.destination_vault_address", "destinations.chain_id"]
#         ),
#     )
