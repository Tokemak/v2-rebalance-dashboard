import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import streamlit as st
from multicall import Call

from mainnet_launch.constants import AutopoolConstants, time_decorator, ALL_AUTOPOOLS, eth_client, ROOT_PRICE_ORACLE
from mainnet_launch.abis import AUTOPOOL_ETH_STRATEGY_ABI, ROOT_PRICE_ORACLE_ABI
from mainnet_launch.get_events import fetch_events
from mainnet_launch.get_state_by_block import (
    get_state_by_one_block,
    build_blocks_to_use,
    get_raw_state_by_blocks,
    safe_normalize_with_bool_success,
    safe_normalize_6_with_bool_success,
    identity_with_bool_success,
)
from mainnet_launch.destinations import get_current_destinations_to_symbol


def fetch_rebalance_events(autopool: AutopoolConstants, blocks: list[int]) -> dict[str, pd.DataFrame]:
    destination_to_symbol = get_current_destinations_to_symbol(max(blocks))

    strategy_contract = eth_client.eth.contract(autopool.autopool_eth_strategy_addr, abi=AUTOPOOL_ETH_STRATEGY_ABI)
    
    rebalance_between_destinations_df = fetch_events(
        strategy_contract.events.RebalanceBetweenDestinations, start_block=min(blocks)
    )
    
    clean_rebalance_df = pd.DataFrame.from_records(
        rebalance_between_destinations_df.apply(
            lambda row: _make_rebalance_between_destination_human_readable(row, destination_to_symbol), axis=1
        )
    )

    clean_rebalance_df["gasCostInETH"] = clean_rebalance_df.apply(
        lambda row: calc_gas_used_by_transaction_in_eth(row["hash"]), axis=1
    )
    
    clean_rebalance_df["flash_borrower_address"] = clean_rebalance_df.apply(
        lambda row: get_flash_borrower_address(row["hash"]), axis=1
    )
    
    if clean_rebalance_df["flash_borrower_address"].nunique() != 1:
        
        raise ValueError('expected only 1 flash borrower address, found more than one', clean_rebalance_df.unique())
    
    flash_borrower_address = clean_rebalance_df["flash_borrower_address"].iloc[0]


def _make_rebalance_between_destination_human_readable(row: dict, destination_to_symbol: dict) -> dict:
    
    predictedAnnualizedGain = (row["predictedAnnualizedGain"]) / 1e18
    predicted_gain_during_swap_cost_off_set_period = predictedAnnualizedGain * (row["swapOffsetPeriod"] / 365)

    swapCost = row["valueStats"][4] / 1e18
    slippage = row["valueStats"][5] / 1e18
    in_destination = destination_to_symbol[row["inSummaryStats"][0]]
    out_destination = destination_to_symbol[row["outSummaryStats"][0]]

    out_compositeReturn = 100 * row["outSummaryStats"][9] / 1e18
    in_compositeReturn = 100 * row["inSummaryStats"][9] / 1e18
    apr_delta = in_compositeReturn - out_compositeReturn
    inEthValue = row["valueStats"][2] / 1e18
    outEthValue = row["valueStats"][3] / 1e18

    predicted_increase_after_swap_cost = predicted_gain_during_swap_cost_off_set_period - swapCost
    date = pd.to_datetime(eth_client.eth.get_block(row["block"]).timestamp, unit="s")

    break_even_days = swapCost / (predictedAnnualizedGain / 365)
    offset_period = row["swapOffsetPeriod"]

    return {
        "date": date,
        "block": row["block"],
        "break_even_days": break_even_days,
        "swapCost": swapCost,
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
    }
    # from 0x3D1f51c23d1586c062B4bECa120bfCAf064e0cdC : EAO, that calls the flash borrow solver


def calc_gas_used_by_transaction_in_eth(tx_hash: str) -> float:
    tx_receipt = eth_client.eth.get_transaction_receipt(tx_hash)
    tx = eth_client.eth.get_transaction(tx_hash)
    return float(eth_client.fromWei(tx["gasPrice"] * tx_receipt["gasUsed"], "ether"))


def getPriceInEth_call(name: str, token_address: str) -> Call:
    return Call(
        ROOT_PRICE_ORACLE,
        ["getPriceInEth(address)(uint256)", token_address],
        [(name, safe_normalize_with_bool_success)],
    )


def get_flash_borrower_address(tx_hash:str) -> str:
    # get the address of the flash borrower that did this rebalance. the value the accumulates here is the solver profit
    return eth_client.eth.get_transaction(tx_hash)['to']


def build_solver_profit_calls(clean_rebalance_df:pd.DataFrame, flash_borrower_address:str) -> list[Call]:
    """To calc the solver profit we need to get the eth value of all tokens that the root price oracle can price"""
    root_price_oracle_contract = eth_client.eth.contract(ROOT_PRICE_ORACLE, abi=ROOT_PRICE_ORACLE_ABI)
    tokens:list[str] = fetch_events(root_price_oracle_contract.events.TokenRegistered)["token"].values

    symbol_calls = [Call(t, ["symbol()(string)"], [(f"{t}_syumbol", identity_with_bool_success)]) for t in tokens]
    block = int(clean_rebalance_df["block"].max())
    token_address_to_symbol = get_state_by_one_block(symbol_calls, block)

    price_calls = [getPriceInEth_call(token_address_to_symbol[t], t) for t in tokens]
    # add balance of calls
    
    
    
    


def _add_solver_profit_cols(clean_rebalance_df: pd.DataFrame) -> list[Call]:
    """

    Solver profit is defined as

    Solver Profit = ETH value of tokens held by the solver a block right BEFORE a rebalance - ETH value of tokens held by the solver a block right AFTER a rebalance

    NOTE: assumes that there is no more than one rebalance per block

    """



    # price_calls =
    # for token_address in tokens:
    #     symbol = token_address_to_symbol[token_address]
    #     deciamls = token_address_to_decimals[token_address]

    # price_calls = []
    # for token_addr, symbol in address_to_symbol.items():
    #     if symbol is not None:
    #         balance_of_calls.append(
    #             Call(
    #                 token_addr,
    #                 ["balanceOf(address)(uint256)", FLASH_BORROW_SOLVER],
    #                 [(symbol, safe_normalize_with_bool_success)],
    #             )
    #         )
    #         price_calls.append(getPriceInEth_call(symbol, token_addr))

    # value_before_df = _build_value_held_by_solver(balance_of_calls, price_calls, clean_rebalance_df["block"] - 1)
    # value_after_df = _build_value_held_by_solver(balance_of_calls, price_calls, clean_rebalance_df["block"])

    # clean_rebalance_df["before_rebalance_eth_value_of_solver"] = value_before_df["total_eth_value"].values
    # clean_rebalance_df["after_rebalance_eth_value_of_solver"] = value_after_df["total_eth_value"].values
    # clean_rebalance_df["solver_profit"] = (
    #     clean_rebalance_df["after_rebalance_eth_value_of_solver"]
    #     - clean_rebalance_df["before_rebalance_eth_value_of_solver"]
    # )
    # return clean_rebalance_df


if __name__ == "__main__":
    blocks = build_blocks_to_use()
    fetch_rebalance_events(ALL_AUTOPOOLS[0], blocks)

    pass
# def _build_value_held_by_solver(balance_of_calls, price_calls, blocks):
#     blocks = [int(b) for b in blocks]
#     balance_of_df = get_raw_state_by_blocks(balance_of_calls, blocks)
#     price_df = get_raw_state_by_blocks(price_calls, blocks)  # might want to fill na because of
#     price_df["ETH"] = 1.0
#     price_df = price_df[balance_of_df.columns]
#     eth_value_held_by_flash_solver_df = price_df * balance_of_df
#     eth_value_held_by_flash_solver_df["total_eth_value"] = eth_value_held_by_flash_solver_df.sum(axis=1)
#     return eth_value_held_by_flash_solver_df

