import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import streamlit as st
import pandas as pd
from multicall import Call

from mainnet_launch.constants import (
    CACHE_TIME,
    AutopoolConstants,
    ROOT_PRICE_ORACLE,
    ChainData,
)
from mainnet_launch.abis.abis import ROOT_PRICE_ORACLE_ABI
from mainnet_launch.data_fetching.get_events import fetch_events
from mainnet_launch.data_fetching.get_state_by_block import (
    get_state_by_one_block,
    build_blocks_to_use,
    get_raw_state_by_blocks,
    safe_normalize_with_bool_success,
    identity_with_bool_success,
)
from mainnet_launch.destinations import get_destination_details

from mainnet_launch.data_fetching.add_info_to_dataframes import (
    add_timestamp_to_df_with_block_column,
    add_transaction_gas_info_to_df_with_tx_hash,
)
from mainnet_launch.autopool_diagnostics.compute_rebalance_cost import fetch_rebalance_events_actual_amounts


@st.cache_data(ttl=CACHE_TIME)
def fetch_rebalance_events_df(autopool: AutopoolConstants) -> pd.DataFrame:
    clean_rebalance_df = fetch_and_clean_rebalance_between_destination_events(autopool)

    clean_rebalance_df = add_transaction_gas_info_to_df_with_tx_hash(clean_rebalance_df, autopool.chain)

    clean_rebalance_df["flash_borrower_address"] = clean_rebalance_df.apply(
        lambda row: _get_flash_borrower_address(row["hash"], autopool.chain), axis=1
    )

    clean_rebalance_df = _add_solver_profit_cols(clean_rebalance_df, autopool)

    return clean_rebalance_df


@st.cache_data(ttl=CACHE_TIME)
def fetch_and_clean_rebalance_between_destination_events(
    autopool: AutopoolConstants,
) -> pd.DataFrame:  # TODO rename, this is all reblances
    rebalance_df = fetch_rebalance_events_actual_amounts(autopool)
    blocks = build_blocks_to_use(autopool.chain)
    destination_details = get_destination_details(autopool, blocks)
    destination_vault_address_to_symbol = {dest.vaultAddress: dest.vault_name for dest in destination_details}

    def _make_rebalance_between_destination_human_readable(
        row: dict,
    ) -> dict:

        swapCost = float(row["swap_cost"])

        inEthValue = row["spot_value_in"]
        outEthValue = row["spot_value_out"]
        slippage = swapCost / outEthValue

        in_destination_symbol = destination_vault_address_to_symbol[row["inDestinationVault"]]
        out_destination_symbol = destination_vault_address_to_symbol[row["outDestinationVault"]]
        moveName = f"{out_destination_symbol} -> {in_destination_symbol}"

        if row["event"] == "RebalanceBetweenDestinations":
            predictedAnnualizedGain = float(row["predictedAnnualizedGain"]) / 1e18
            predicted_gain_during_swap_cost_off_set_period = predictedAnnualizedGain * (row["swapOffsetPeriod"] / 365)
            out_compositeReturn = 100 * float(row["outSummaryStats"][9]) / 1e18
            in_compositeReturn = 100 * float(row["inSummaryStats"][9]) / 1e18
            apr_delta = in_compositeReturn - out_compositeReturn
            predicted_increase_after_swap_cost = predicted_gain_during_swap_cost_off_set_period - swapCost
            break_even_days = swapCost / (predictedAnnualizedGain / 365)
            offset_period = row["swapOffsetPeriod"]

            return {
                "block": row["block"],
                "break_even_days": break_even_days,
                "swapCost": swapCost,
                "swapCostIdle": 0,
                "swapCostChurn": swapCost,
                "apr_delta": apr_delta,
                "out_compositeReturn": out_compositeReturn,
                "in_compositeReturn": in_compositeReturn,
                "predicted_increase_after_swap_cost": predicted_increase_after_swap_cost,
                "predicted_gain_during_swap_cost_off_set_period": predicted_gain_during_swap_cost_off_set_period,
                "inEthValue": inEthValue,
                "outEthValue": outEthValue,
                "out_destination": row["outDestinationVault"],
                "in_destination": row["inDestinationVault"],
                "offset_period": offset_period,
                "slippage": slippage,
                "hash": row["hash"],
                "moveName": moveName,
                "event": row["event"],
            }

        elif row["event"] == "RebalanceToIdle":

            out_compositeReturn = 100 * float(row["outSummary"][9]) / 1e18
            in_compositeReturn = 0
            apr_delta = in_compositeReturn - out_compositeReturn
            return {
                "block": row["block"],
                "break_even_days": None,
                "swapCost": swapCost,
                "swapCostIdle": swapCost,
                "swapCostChurn": 0,
                "apr_delta": apr_delta,
                "out_compositeReturn": out_compositeReturn,
                "in_compositeReturn": in_compositeReturn,
                "predicted_increase_after_swap_cost": None,
                "predicted_gain_during_swap_cost_off_set_period": None,
                "inEthValue": inEthValue,
                "outEthValue": outEthValue,
                "out_destination": row["outDestinationVault"],
                "in_destination": row["inDestinationVault"],
                "offset_period": None,
                "slippage": slippage,
                "hash": row["hash"],
                "moveName": moveName,
                "event": row["event"],
            }
        else:
            raise ValueError("Unexpected event name", row["event"])

    clean_rebalance_df = pd.DataFrame.from_records(
        rebalance_df.apply(lambda row: _make_rebalance_between_destination_human_readable(row), axis=1)
    )
    clean_rebalance_df = add_timestamp_to_df_with_block_column(clean_rebalance_df, autopool.chain)
    return clean_rebalance_df


def getPriceInEth_call(name: str, token_address: str, chain: ChainData) -> Call:
    return Call(
        ROOT_PRICE_ORACLE(chain),
        ["getPriceInEth(address)(uint256)", token_address],
        [(name, safe_normalize_with_bool_success)],
    )


def _get_flash_borrower_address(tx_hash: str, chain: ChainData) -> str:
    # get the address of the flash borrower that did this rebalance
    return chain.client.eth.get_transaction(tx_hash)["to"]


def _add_solver_profit_cols(clean_rebalance_df: pd.DataFrame, autopool: AutopoolConstants):
    all_flash_borrowers = clean_rebalance_df["flash_borrower_address"].unique()
    rebalance_dfs = []
    for flash_borrower_address in all_flash_borrowers:
        limited_clean_rebalance_df = clean_rebalance_df[
            clean_rebalance_df["flash_borrower_address"] == flash_borrower_address
        ].copy()
        limited_clean_rebalance_df = _add_solver_profit_cols_by_flash_borrower(
            limited_clean_rebalance_df, flash_borrower_address, autopool.chain
        )
        rebalance_dfs.append(limited_clean_rebalance_df)

    all_clean_rebalance_df = pd.concat(rebalance_dfs, axis=0)
    return all_clean_rebalance_df


def _add_solver_profit_cols_by_flash_borrower(
    limited_clean_rebalance_df: pd.DataFrame, flash_borrower_address: str, chain: ChainData
) -> list[Call]:
    """
    Solver profit: ETH value held by the solver AFTER a rebalance - ETH value held by the solver BEFORE a rebalance
    """
    root_price_oracle_contract = chain.client.eth.contract(ROOT_PRICE_ORACLE(chain), abi=ROOT_PRICE_ORACLE_ABI)
    tokens: list[str] = fetch_events(root_price_oracle_contract.events.TokenRegistered)["token"].values

    symbol_calls = [Call(t, ["symbol()(string)"], [(t, identity_with_bool_success)]) for t in tokens]
    block = int(limited_clean_rebalance_df["block"].max())
    token_address_to_symbol = get_state_by_one_block(symbol_calls, block, chain)

    price_calls = [getPriceInEth_call(token_address_to_symbol[t], t, chain) for t in tokens]
    balance_of_calls = [
        Call(
            t,
            ["balanceOf(address)(uint256)", flash_borrower_address],
            [(token_address_to_symbol[t], safe_normalize_with_bool_success)],
        )
        for t in tokens
    ]

    # compare the ETH value in the solver before and after a rebalance
    value_before_df = _build_value_held_by_solver(
        balance_of_calls, price_calls, limited_clean_rebalance_df["block"] - 1, chain
    )
    value_after_df = _build_value_held_by_solver(
        balance_of_calls, price_calls, limited_clean_rebalance_df["block"], chain
    )

    limited_clean_rebalance_df["before_rebalance_eth_value_of_solver"] = value_before_df["total_eth_value"].values
    limited_clean_rebalance_df["after_rebalance_eth_value_of_solver"] = value_after_df["total_eth_value"].values
    limited_clean_rebalance_df["solver_profit"] = (
        limited_clean_rebalance_df["after_rebalance_eth_value_of_solver"]
        - limited_clean_rebalance_df["before_rebalance_eth_value_of_solver"]
    )

    return limited_clean_rebalance_df


def _build_value_held_by_solver(
    balance_of_calls: list[Call], price_calls: list[Call], blocks: list[int], chain: ChainData
) -> pd.DataFrame:
    balance_of_df = get_raw_state_by_blocks(balance_of_calls, blocks, chain)
    price_df = get_raw_state_by_blocks(price_calls, blocks, chain)
    price_df["ETH"] = 1.0
    # ensure the columns are in the right order
    eth_value_held_by_flash_solver_df = price_df[balance_of_df.columns] * balance_of_df[balance_of_df.columns]
    eth_value_held_by_flash_solver_df["total_eth_value"] = eth_value_held_by_flash_solver_df.sum(axis=1)
    return eth_value_held_by_flash_solver_df


if __name__ == "__main__":
    from mainnet_launch.constants import CACHE_TIME, ALL_AUTOPOOLS, BAL_ETH, AUTO_LRT

    clean_rebalance_df = fetch_rebalance_events_df(AUTO_LRT)

    pass
