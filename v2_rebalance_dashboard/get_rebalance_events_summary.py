from v2_rebalance_dashboard.get_events import fetch_events
from v2_rebalance_dashboard.constants import (
    ROOT_PRICE_ORACLE_ABI,
    ROOT_PRICE_ORACLE,
    balETH_AUTOPOOL_ETH_STRATEGY_ADDRESS,
    eth_client,
    ROOT_DIR,
)

from dataclasses import dataclass
import pandas as pd
import json
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st
from multicall import Call
from v2_rebalance_dashboard.get_state_by_block import (
    safe_normalize_with_bool_success,
    identity_with_bool_success,
    sync_get_raw_state_by_block_one_block,
    to_str_with_bool_success,
    sync_safe_get_raw_state_by_block,
    build_get_address_eth_balance_call,
)


with open(ROOT_DIR / "vault_abi.json", "r") as fin:
    autopool_eth_vault_abi = json.load(fin)

with open(ROOT_DIR / "strategy_abi.json", "r") as fin:
    eth_strategy_abi = json.load(fin)

destination_df = pd.read_csv(ROOT_DIR / "vaults.csv", index_col=0)
destination_vault_to_name = {
    str(vault_address).lower(): name[22:]
    for vault_address, name in zip(destination_df["vaultAddress"], destination_df["name"])
}
destination_vault_to_name["0x72cf6d7c85ffd73f18a83989e7ba8c1c30211b73"] = "balETH idle"

FLASH_BORROW_SOLVER = "0x2C26808b567BA224652f4eB20D45df4bccC29470"

# struct RebalanceParams {
#     address destinationIn; 0
#     address tokenIn; 1
#     uint256 amountIn; 2
#     address destinationOut; 3
#     address tokenOut; 4
#     uint256 amountOut; 5
# }

# struct SummaryStats {
#     address destination; 0
#     uint256 baseApr; 1
#     uint256 feeApr; 2
#     uint256 incentiveApr; 3
#     uint256 safeTotalSupply; 4
#     int256 priceReturn; 5
#     int256 maxDiscount; 6
#     int256 maxPremium; 7
#     uint256 ownedShares; 8
#     int256 compositeReturn; 9
#     uint256 pricePerShare; 10
# }

# struct RebalanceValueStats {
#     uint256 inPrice; 0
#     uint256 outPrice;1
#     uint256 inEthValue; 2
#     uint256 outEthValue; 3
#     uint256 swapCost; 4
#     uint256 slippage; 5
# }


def make_rebalance_human_readable(row: dict):
    predictedAnnualizedGain = (row["predictedAnnualizedGain"]) / 1e18
    predicted_gain_during_swap_cost_off_set_period = predictedAnnualizedGain * (row["swapOffsetPeriod"] / 365)

    swapCost = row["valueStats"][4] / 1e18
    slippage = row["valueStats"][5] / 1e18
    in_destination = destination_vault_to_name[str.lower(row["inSummaryStats"][0])]
    out_destination = destination_vault_to_name[str.lower(row["outSummaryStats"][0])]

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


def calc_gas_used_by_transaction_in_eth(tx_hash: str) -> float:
    tx_receipt = eth_client.eth.get_transaction_receipt(tx_hash)
    tx = eth_client.eth.get_transaction(tx_hash)
    return eth_client.fromWei(tx["gasPrice"] * tx_receipt["gasUsed"], "ether")


def getPriceInEth_call(name: str, token_address: str) -> Call:
    return Call(
        ROOT_PRICE_ORACLE,
        ["getPriceInEth(address)(uint256)", token_address],
        [(name, safe_normalize_with_bool_success)],
    )


def _build_value_held_by_solver(balance_of_calls, price_calls, blocks):
    blocks = [int(b) for b in blocks]
    balance_of_df = sync_safe_get_raw_state_by_block(balance_of_calls, blocks)
    price_df = sync_safe_get_raw_state_by_block(price_calls, blocks).fillna(0)  # just usdc is 0s
    price_df["ETH"] = 1.0
    price_df = price_df[balance_of_df.columns]
    eth_value_held_by_flash_solver_df = price_df * balance_of_df
    eth_value_held_by_flash_solver_df["total_eth_value"] = eth_value_held_by_flash_solver_df.sum(axis=1)
    return eth_value_held_by_flash_solver_df


def _add_solver_profit_cols(clean_rebalance_df: pd.DataFrame) -> list[Call]:

    root_price_oracle_contract = eth_client.eth.contract(ROOT_PRICE_ORACLE, abi=ROOT_PRICE_ORACLE_ABI)
    tokens = fetch_events(root_price_oracle_contract.events.TokenRegistered)["token"].values

    symbol_calls = [Call(t, ["symbol()(string)"], [(t, to_str_with_bool_success)]) for t in tokens]
    address_to_symbol = sync_get_raw_state_by_block_one_block(symbol_calls, 20651330)
    balance_of_calls = [build_get_address_eth_balance_call("ETH", FLASH_BORROW_SOLVER)]

    price_calls = []
    for token_addr, symbol in address_to_symbol.items():
        if symbol is not None:
            balance_of_calls.append(
                Call(
                    token_addr,
                    ["balanceOf(address)(uint256)", FLASH_BORROW_SOLVER],
                    [(symbol, safe_normalize_with_bool_success)],
                )
            )
            price_calls.append(getPriceInEth_call(symbol, token_addr))

    value_before_df = _build_value_held_by_solver(balance_of_calls, price_calls, clean_rebalance_df["block"] - 1)
    value_after_df = _build_value_held_by_solver(balance_of_calls, price_calls, clean_rebalance_df["block"])

    clean_rebalance_df["before_rebalance_eth_value_of_solver"] = value_before_df["total_eth_value"].values
    clean_rebalance_df["after_rebalance_eth_value_of_solver"] = value_after_df["total_eth_value"].values
    clean_rebalance_df["solver_profit"] = (
        clean_rebalance_df["after_rebalance_eth_value_of_solver"]
        - clean_rebalance_df["before_rebalance_eth_value_of_solver"]
    )
    return clean_rebalance_df


def _fetch_rebalance_events_df() -> pd.DataFrame:
    strategy_contract = eth_client.eth.contract(balETH_AUTOPOOL_ETH_STRATEGY_ADDRESS, abi=eth_strategy_abi)
    rebalance_events = fetch_events(strategy_contract.events.RebalanceBetweenDestinations)
    clean_rebalance_df = pd.DataFrame.from_records(
        rebalance_events.apply(lambda row: make_rebalance_human_readable(row), axis=1)
    )

    clean_rebalance_df["gasCostInETH"] = clean_rebalance_df.apply(
        lambda row: calc_gas_used_by_transaction_in_eth(row["hash"]), axis=1
    )
    clean_rebalance_df = _add_solver_profit_cols(clean_rebalance_df)
    return clean_rebalance_df


def _add_composite_return_figures(clean_rebalance_df: pd.DataFrame, fig: go.Figure):
    fig.add_trace(
        go.Bar(x=clean_rebalance_df["date"], y=clean_rebalance_df["out_compositeReturn"], name="Out Composite Return"),
        row=1,
        col=1,
    )
    fig.add_trace(
        go.Bar(x=clean_rebalance_df["date"], y=clean_rebalance_df["in_compositeReturn"], name="In Composite Return"),
        row=1,
        col=1,
    )
    fig.update_yaxes(title_text="Return (%)", row=1, col=1)


def _add_in_out_eth_value(clean_rebalance_df: pd.DataFrame, fig: go.Figure):
    fig.add_trace(
        go.Bar(x=clean_rebalance_df["date"], y=clean_rebalance_df["outEthValue"], name="Out ETH Value"), row=2, col=1
    )
    fig.add_trace(
        go.Bar(x=clean_rebalance_df["date"], y=clean_rebalance_df["inEthValue"], name="In ETH Value"), row=2, col=1
    )

    fig.update_yaxes(title_text="ETH", row=2, col=1)


def _add_predicted_gain_and_swap_cost(clean_rebalance_df: pd.DataFrame, fig: go.Figure):
    fig.add_trace(
        go.Bar(
            x=clean_rebalance_df["date"],
            y=clean_rebalance_df["predicted_gain_during_swap_cost_off_set_period"],
            name="Predicted Gain",
        ),
        row=3,
        col=1,
    )
    fig.add_trace(
        go.Bar(x=clean_rebalance_df["date"], y=clean_rebalance_df["swapCost"], name="Swap Cost"), row=3, col=1
    )

    fig.update_yaxes(title_text="ETH", row=3, col=1)


def _add_swap_cost_percent(clean_rebalance_df: pd.DataFrame, fig: go.Figure):
    swap_cost_percentage = (clean_rebalance_df["slippage"]) * 100
    fig.add_trace(
        go.Bar(x=clean_rebalance_df["date"], y=swap_cost_percentage, name="Swap Cost Percentage"), row=4, col=1
    )
    fig.update_yaxes(title_text="Swap Cost (%)", row=4, col=1)


def _add_break_even_days_and_offset_period(clean_rebalance_df: pd.DataFrame, fig: go.Figure):
    fig.add_trace(
        go.Bar(x=clean_rebalance_df["date"], y=clean_rebalance_df["break_even_days"], name="Break Even Days"),
        row=5,
        col=1,
    )
    fig.add_trace(
        go.Bar(x=clean_rebalance_df["date"], y=clean_rebalance_df["offset_period"], name="Offset Period"), row=5, col=1
    )
    fig.update_yaxes(title_text="Days", row=5, col=1)


def _add_solver_profit(clean_rebalance_df: pd.DataFrame, fig: go.Figure):
    fig.add_trace(
        go.Bar(x=clean_rebalance_df["date"], y=clean_rebalance_df["solver_profit"], name="Solver Profit Before Gas"),
        row=6,
        col=1,
    )

    fig.add_trace(
        go.Bar(x=clean_rebalance_df["date"], y=clean_rebalance_df["gasCostInETH"], name="Solver Gas Cost in ETH"),
        row=6,
        col=1,
    )

    solver_profit_after_gas_costs = clean_rebalance_df["solver_profit"].astype(float) - clean_rebalance_df[
        "gasCostInETH"
    ].astype(float)

    fig.add_trace(
        go.Bar(x=clean_rebalance_df["date"], y=solver_profit_after_gas_costs, name="Solver Profit After Gas"),
        row=6,
        col=1,
    )
    fig.update_yaxes(title_text="ETH", row=6, col=1)


def _make_plots(clean_rebalance_df: pd.DataFrame) -> go.Figure:
    fig = make_subplots(
        rows=6,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.08,
        subplot_titles=(
            "Composite Returns",
            "in/out ETH Values",
            "Swap Cost, Predicted Gain",
            "Swap Cost as Percentage of Out ETH Value",
            "Break Even Days and Offset Period",
            "Solver Profit and Gas",
        ),
    )

    _add_composite_return_figures(clean_rebalance_df, fig)
    _add_in_out_eth_value(clean_rebalance_df, fig)
    _add_predicted_gain_and_swap_cost(clean_rebalance_df, fig)
    _add_swap_cost_percent(clean_rebalance_df, fig)
    _add_break_even_days_and_offset_period(clean_rebalance_df, fig)
    _add_solver_profit(clean_rebalance_df, fig)

    # Update layout
    fig.update_layout(
        height=6 * 400,
        width=1000,
        title_text="",
        plot_bgcolor="white",
        paper_bgcolor="white",
        font=dict(color="black"),
    )

    # Update x-axes
    fig.update_xaxes(
        title_text="Date",
        row=6,
        col=1,
        showgrid=True,
        gridwidth=1,
        gridcolor="lightgray",
        zeroline=True,
        zerolinewidth=1,
        zerolinecolor="black",
    )

    # Update y-axes
    fig.update_yaxes(
        showgrid=True,
        gridwidth=1,
        gridcolor="lightgray",
        zeroline=True,
        zerolinewidth=1,
        zerolinecolor="black",
    )
    return fig


@st.cache_data(ttl=12 * 3600)
def fetch_clean_rebalance_events(autopool_name="balETH"):
    if autopool_name != "balETH":
        raise ValueError("only for balETH")

    clean_rebalance_df = _fetch_rebalance_events_df()
    fig = _make_plots(clean_rebalance_df)
    return fig


if __name__ == "__main__":
    fig = fetch_clean_rebalance_events()
    fig.show()
