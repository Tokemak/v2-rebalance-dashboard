import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import streamlit as st
from multicall import Call

from mainnet_launch.constants import (
    CACHE_TIME,
    AutopoolConstants,
    time_decorator,
    ALL_AUTOPOOLS,
    eth_client,
    ROOT_PRICE_ORACLE,
)
from mainnet_launch.abis.abis import AUTOPOOL_ETH_STRATEGY_ABI, ROOT_PRICE_ORACLE_ABI
from mainnet_launch.data_fetching.get_events import fetch_events
from mainnet_launch.data_fetching.get_state_by_block import (
    get_state_by_one_block,
    build_blocks_to_use,
    get_raw_state_by_blocks,
    safe_normalize_with_bool_success,
    safe_normalize_6_with_bool_success,
    identity_with_bool_success,
)
from mainnet_launch.destinations import get_destination_details

from mainnet_launch.data_fetching.add_info_to_dataframes import add_timestamp_to_df_with_block_column


@st.cache_data(ttl=CACHE_TIME)
def fetch_rebalance_events_df(autopool: AutopoolConstants) -> pd.DataFrame:
    clean_rebalance_df = fetch_and_clean_rebalance_between_destination_events(autopool)

    clean_rebalance_df["gasCostInETH"] = clean_rebalance_df.apply(
        lambda row: _calc_gas_used_by_transaction_in_eth(row["hash"]), axis=1
    )

    clean_rebalance_df["flash_borrower_address"] = clean_rebalance_df.apply(
        lambda row: _get_flash_borrower_address(row["hash"]), axis=1
    )

    clean_rebalance_df = _add_solver_profit_cols(clean_rebalance_df)

    return clean_rebalance_df


@st.cache_data(ttl=CACHE_TIME)
def fetch_and_clean_rebalance_between_destination_events(autopool: AutopoolConstants) -> pd.DataFrame:
    strategy_contract = eth_client.eth.contract(autopool.autopool_eth_strategy_addr, abi=AUTOPOOL_ETH_STRATEGY_ABI)

    rebalance_between_destinations_df = fetch_events(strategy_contract.events.RebalanceBetweenDestinations)
    destination_details = get_destination_details()
    destination_vault_address_to_symbol = {dest.vaultAddress: dest.vault_name for dest in destination_details}

    def _make_rebalance_between_destination_human_readable(
        row: dict,
    ) -> dict:

        predictedAnnualizedGain = (row["predictedAnnualizedGain"]) / 1e18
        predicted_gain_during_swap_cost_off_set_period = predictedAnnualizedGain * (row["swapOffsetPeriod"] / 365)

        swapCost = row["valueStats"][4] / 1e18 # check if this
        slippage = row["valueStats"][5] / 1e18
        in_destination = destination_vault_address_to_symbol[eth_client.toChecksumAddress(row["inSummaryStats"][0])]
        out_destination = destination_vault_address_to_symbol[eth_client.toChecksumAddress(row["outSummaryStats"][0])]

        out_compositeReturn = 100 * row["outSummaryStats"][9] / 1e18
        in_compositeReturn = 100 * row["inSummaryStats"][9] / 1e18
        apr_delta = in_compositeReturn - out_compositeReturn
        
        raw_in, raw_out, txHASH = row["valueStats"][2], row["valueStats"][3], row["hash"]
        print(raw_in, raw_out)
        inEthValue = row["valueStats"][2] / 1e18
        outEthValue = row["valueStats"][3] / 1e18

        predicted_increase_after_swap_cost = predicted_gain_during_swap_cost_off_set_period - swapCost

        break_even_days = swapCost / (predictedAnnualizedGain / 365)
        offset_period = row["swapOffsetPeriod"]

        move_name = f"{out_destination} -> {in_destination}"
        if "tokemak" in move_name:
            swapCostIdle = swapCost
            swapCostChurn = 0
        else:
            swapCostIdle = 0
            swapCostChurn = swapCost

        return {
            "block": row["block"],
            "break_even_days": break_even_days,
            "swapCost": swapCost,
            "swapCostIdle": swapCostIdle,
            "swapCostChurn": swapCostChurn,
            "apr_delta": apr_delta,
            "out_compositeReturn": out_compositeReturn,
            "in_compositeReturn": in_compositeReturn,
            "predicted_increase_after_swap_cost": predicted_increase_after_swap_cost,
            "predicted_gain_during_swap_cost_off_set_period": predicted_gain_during_swap_cost_off_set_period,
            "inEthValue": inEthValue,
            "outEthValue": outEthValue,
            "out_destination": out_destination,
            "in_destination": in_destination,
            "offset_period": offset_period,
            "slippage": slippage,
            "hash": row["hash"],
            "moveName": move_name,
        }

    clean_rebalance_df = pd.DataFrame.from_records(
        rebalance_between_destinations_df.apply(
            lambda row: _make_rebalance_between_destination_human_readable(row), axis=1
        )
    )
    clean_rebalance_df = add_timestamp_to_df_with_block_column(clean_rebalance_df)
    return clean_rebalance_df


def _calc_gas_used_by_transaction_in_eth(tx_hash: str) -> float:
    tx_receipt = eth_client.eth.get_transaction_receipt(tx_hash)
    tx = eth_client.eth.get_transaction(tx_hash)
    return float(eth_client.fromWei(tx["gasPrice"] * tx_receipt["gasUsed"], "ether"))


def getPriceInEth_call(name: str, token_address: str) -> Call:
    return Call(
        ROOT_PRICE_ORACLE,
        ["getPriceInEth(address)(uint256)", token_address],
        [(name, safe_normalize_with_bool_success)],
    )


def _get_flash_borrower_address(tx_hash: str) -> str:
    # get the address of the flash borrower that did this rebalance
    return eth_client.eth.get_transaction(tx_hash)["to"]


def _add_solver_profit_cols(clean_rebalance_df: pd.DataFrame):

    all_flash_borrowers = clean_rebalance_df["flash_borrower_address"].unique()
    rebalance_dfs = []
    for flash_borrower_address in all_flash_borrowers:
        limited_clean_rebalance_df = clean_rebalance_df[
            clean_rebalance_df["flash_borrower_address"] == flash_borrower_address
        ].copy()
        limited_clean_rebalance_df = _add_solver_profit_cols_by_flash_borrower(
            limited_clean_rebalance_df, flash_borrower_address
        )
        rebalance_dfs.append(limited_clean_rebalance_df)

    all_clean_rebalance_df = pd.concat(rebalance_dfs, axis=0)
    return all_clean_rebalance_df


def _add_solver_profit_cols_by_flash_borrower(
    limited_clean_rebalance_df: pd.DataFrame, flash_borrower_address: str
) -> list[Call]:
    """
    Solver profit: ETH value held by the solver AFTER a rebalance - ETH value held by the solver BEFORE a rebalance
    """
    root_price_oracle_contract = eth_client.eth.contract(ROOT_PRICE_ORACLE, abi=ROOT_PRICE_ORACLE_ABI)
    tokens: list[str] = fetch_events(root_price_oracle_contract.events.TokenRegistered)["token"].values

    symbol_calls = [Call(t, ["symbol()(string)"], [(t, identity_with_bool_success)]) for t in tokens]
    block = int(limited_clean_rebalance_df["block"].max())
    token_address_to_symbol = get_state_by_one_block(symbol_calls, block)

    price_calls = [getPriceInEth_call(token_address_to_symbol[t], t) for t in tokens]
    balance_of_calls = [
        Call(
            t,
            ["balanceOf(address)(uint256)", flash_borrower_address],
            [(token_address_to_symbol[t], safe_normalize_with_bool_success)],
        )
        for t in tokens
    ]

    value_before_df = _build_value_held_by_solver(
        balance_of_calls, price_calls, limited_clean_rebalance_df["block"] - 1
    )
    value_after_df = _build_value_held_by_solver(balance_of_calls, price_calls, limited_clean_rebalance_df["block"])

    limited_clean_rebalance_df["before_rebalance_eth_value_of_solver"] = value_before_df["total_eth_value"].values
    limited_clean_rebalance_df["after_rebalance_eth_value_of_solver"] = value_after_df["total_eth_value"].values
    limited_clean_rebalance_df["solver_profit"] = (
        limited_clean_rebalance_df["after_rebalance_eth_value_of_solver"]
        - limited_clean_rebalance_df["before_rebalance_eth_value_of_solver"]
    )

    return limited_clean_rebalance_df


def _build_value_held_by_solver(balance_of_calls, price_calls, blocks):
    blocks = [int(b) for b in blocks]
    balance_of_df = get_raw_state_by_blocks(balance_of_calls, blocks)
    price_df = get_raw_state_by_blocks(price_calls, blocks)
    price_df["ETH"] = 1.0
    price_df = price_df[balance_of_df.columns]
    eth_value_held_by_flash_solver_df = price_df * balance_of_df
    eth_value_held_by_flash_solver_df["total_eth_value"] = eth_value_held_by_flash_solver_df.sum(axis=1)
    return eth_value_held_by_flash_solver_df


if __name__ == "__main__":
    from mainnet_launch.constants import CACHE_TIME, ALL_AUTOPOOLS, BAL_ETH

    clean_rebalance_df = fetch_rebalance_events_df(BAL_ETH)

    pass
