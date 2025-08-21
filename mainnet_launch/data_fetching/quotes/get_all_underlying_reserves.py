"""Get the reserves of an autopool at this block"""

from web3 import Web3
import pandas as pd
from multicall import Call

from mainnet_launch.pages.autopool_diagnostics.lens_contract import (
    get_full_destination_pools_and_destinations_at_one_block,
)
from mainnet_launch.data_fetching.get_state_by_block import (
    get_state_by_one_block,
    identity_with_bool_success,
    safe_normalize_with_bool_success,
)
from mainnet_launch.database.schema.full import Destinations, Tokens
from mainnet_launch.database.schema.postgres_operations import get_full_table_as_df
from mainnet_launch.constants.constants import ChainData, ALL_AUTOPOOLS, AutopoolConstants, time_decorator


def get_eth_value_by_destination_by_autopool(lens_contract_data: dict):
    dests = []
    autopool_symbol_to_autopool_constant = {a.symbol: a for a in ALL_AUTOPOOLS}

    for autopool_len_contract_data, list_of_destinations in zip(
        lens_contract_data["autopools"], lens_contract_data["destinations"]
    ):
        if autopool_len_contract_data["symbol"] in autopool_symbol_to_autopool_constant:
            autopool: AutopoolConstants = autopool_symbol_to_autopool_constant[autopool_len_contract_data["symbol"]]
            for dest in list_of_destinations:
                dest["autopool_base_asset_decimals"] = autopool.base_asset_decimals
                dest["autopool_symbol"] = autopool.symbol
                dests.append(dest)
        else:
            # skip autoS for now
            pass

    return pd.DataFrame(dests)[
        ["vaultAddress", "autoPoolOwnsShares", "actualLPTotalSupply", "autopool_base_asset_decimals", "autopool_symbol"]
    ]


def get_underlying_reserves_by_block(lens_contract_df: pd.DataFrame, block: int, chain: ChainData) -> dict:
    calls = [
        Call(
            destination_vault_address,
            "underlyingReserves()(address[],uint256[])",
            [
                ((destination_vault_address, "underlyingReserves_tokens"), identity_with_bool_success),
                ((destination_vault_address, "underlyingReserves_amounts"), identity_with_bool_success),
            ],
        )
        for destination_vault_address in lens_contract_df["vaultAddress"]
    ]

    acutal_reserves_state = get_state_by_one_block(calls, block, chain)
    return acutal_reserves_state


def get_pools_underlying_and_total_supply(destination_vaults: list[str], block: int, chain: ChainData) -> dict:
    underlyingTotalSupply_calls = [
        Call(
            destination_vault_address,
            "underlyingTotalSupply()(uint256)",
            [((destination_vault_address, "underlyingTotalSupply"), identity_with_bool_success)],
        )
        for destination_vault_address in destination_vaults
    ]

    totalSupply_calls = [
        Call(
            destination_vault_address,
            "totalSupply()(uint256)",
            [((destination_vault_address, "totalSupply"), identity_with_bool_success)],
        )
        for destination_vault_address in destination_vaults
    ]

    getPool_calls = [
        Call(
            destination_vault_address,
            "getPool()(address)",
            [((destination_vault_address, "getPool"), identity_with_bool_success)],
        )
        for destination_vault_address in destination_vaults
    ]

    state = get_state_by_one_block([*underlyingTotalSupply_calls, *totalSupply_calls, *getPool_calls], block, chain)
    return state


def fetch_raw_amounts_by_destination(block: int, chain: ChainData) -> pd.DataFrame:
    """Read onchain the raw quantites of each asset we hold in each autopool at this block."""
    data = get_full_destination_pools_and_destinations_at_one_block(chain, block)

    lens_contract_df = get_eth_value_by_destination_by_autopool(data)

    acutal_reserves_state = get_underlying_reserves_by_block(lens_contract_df, block, chain)

    # 'debtValueHeldByVault', might want for sanity check
    lens_contract_df["portion_ownership"] = (
        lens_contract_df["autoPoolOwnsShares"] / lens_contract_df["actualLPTotalSupply"]
    )
    lens_contract_df["reserve_amounts"] = lens_contract_df["vaultAddress"].apply(
        lambda x: acutal_reserves_state[(x, "underlyingReserves_amounts")]
    )
    lens_contract_df["reserve_tokens"] = lens_contract_df["vaultAddress"].apply(
        lambda x: acutal_reserves_state[(x, "underlyingReserves_tokens")]
    )

    raw_base_token_value_by_destination = []

    def _extract_proportional_ownership_of_reserve_tokens_in_underlying_pools(row: pd.Series):
        for reserve_amount, reserve_token_address in zip(row["reserve_amounts"], row["reserve_tokens"]):
            raw_base_token_value_by_destination.append(
                {
                    "token_address": Web3.toChecksumAddress(reserve_token_address),
                    "reserve_amount": int(int(reserve_amount) * row["portion_ownership"]),
                    "vault_address": row["vaultAddress"],
                    "autopool_symbol": row["autopool_symbol"],
                }
            )

    lens_contract_df.apply(
        lambda row: _extract_proportional_ownership_of_reserve_tokens_in_underlying_pools(row), axis=1
    )
    reserve_token_ownership_df = pd.DataFrame.from_records(raw_base_token_value_by_destination)

    balancer_tokens_df = get_full_table_as_df(
        Tokens, where_clause=(Tokens.chain_id == chain.chain_id) & (Tokens.name.contains("Balancer"))
    )

    # exclude balancer pool tokens here
    reserve_token_ownership_df = reserve_token_ownership_df[
        ~reserve_token_ownership_df["token_address"].isin(balancer_tokens_df["token_address"])
    ]
    reserve_token_ownership_df = reserve_token_ownership_df[reserve_token_ownership_df["reserve_amount"] > 0]
    return reserve_token_ownership_df
