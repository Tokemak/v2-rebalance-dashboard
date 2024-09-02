from v2_rebalance_dashboard.get_events import fetch_events
from v2_rebalance_dashboard.constants import balETH_AUTOPOOL_ETH_STRATEGY_ADDRESS, eth_client, ROOT_DIR
import pandas as pd
import json
import plotly.graph_objects as go
from plotly.subplots import make_subplots

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

    break_even_days = swapCost / (predictedAnnualizedGain/365)
    offset_period = row["swapOffsetPeriod"]

    # first_line = slope ,out_compositeReturn, start point (days, eth value) (0, outEthValue)
    # second line = slope ,in_compositeReturn, start point (days, eth value) (0, inEthValue)

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
        "slippage": slippage
    }


def calculate_total_eth_spent(address: str, block: int):
    total_eth_spent = 0
    block = eth_client.eth.get_block(int(block), full_transactions=True)
    for tx in block.transactions:
        if tx["from"].lower() == address.lower():
            # Calculate the total spent for this transaction
            tx_cost = eth_client.fromWei(tx["value"], "ether") + eth_client.fromWei(tx["gasPrice"] * tx["gas"], "ether")
            total_eth_spent += tx_cost

    return total_eth_spent


def fetch_plot_clean_rebalance_events(autopool_name="balETH"):
    if autopool_name != "balETH":
        raise ValueError("only for balETH")

    balETH_solver = "0xad92a528A627F59a12e3EE56246C6F733051f6ca"

    strategy_contract = eth_client.eth.contract(balETH_AUTOPOOL_ETH_STRATEGY_ADDRESS, abi=eth_strategy_abi)
    rebalance_events = fetch_events(strategy_contract.events.RebalanceBetweenDestinations)
    clean_rebalance_df = pd.DataFrame.from_records(
        rebalance_events.apply(lambda row: make_rebalance_human_readable(row), axis=1)
    )
    
    clean_rebalance_df["gasCostInETH"] = clean_rebalance_df.apply(
        lambda row: calculate_total_eth_spent(balETH_solver, row["block"]), axis=1
    )
    clean_rebalance_df.set_index("date", inplace=True)

    # Sort the dataframe by date
    clean_rebalance_df = clean_rebalance_df.sort_values('date')
    
    # Create subplots
    fig = make_subplots(rows=5, cols=1, shared_xaxes=True, vertical_spacing=0.08,
                        subplot_titles=("Composite Returns", "in/out ETH Values",
                                        "Swap Cost and Predicted Gain", 
                                        "Swap Cost as Percentage of Out ETH Value", 
                                        "Break Even Days and Offset Period"))
    
    # Plot 1: out_compositeReturn & in_compositeReturn
    fig.add_trace(go.Bar(x=clean_rebalance_df['date'], y=clean_rebalance_df['out_compositeReturn'],
                         name='Out Composite Return'), row=1, col=1)
    fig.add_trace(go.Bar(x=clean_rebalance_df['date'], y=clean_rebalance_df['in_compositeReturn'],
                         name='In Composite Return'), row=1, col=1)
    
    # Plot 2: predicted_gain_during_swap_cost_offset_period, swapCost, outEthValue, inEthValue
    fig.add_trace(go.Bar(x=clean_rebalance_df['date'], y=clean_rebalance_df['outEthValue'],
                         name='Out ETH Value'), row=2, col=1)
    fig.add_trace(go.Bar(x=clean_rebalance_df['date'], y=clean_rebalance_df['inEthValue'],
                         name='In ETH Value'), row=2, col=1)
    fig.add_trace(go.Bar(x=clean_rebalance_df['date'], y=clean_rebalance_df['predicted_gain_during_swap_cost_off_set_period'],
                         name='Predicted Gain'), row=3, col=1)
    fig.add_trace(go.Bar(x=clean_rebalance_df['date'], y=clean_rebalance_df['swapCost'],
                         name='Swap Cost'), row=3, col=1)
 
    
    # Plot 3: swapCost / outETH * 100
    swap_cost_percentage = (clean_rebalance_df['slippage']) * 100
    fig.add_trace(go.Bar(x=clean_rebalance_df['date'], y=swap_cost_percentage,
                         name='Swap Cost Percentage'), row=4, col=1)
    
    # Plot 4: break_even_days and offset_period
    fig.add_trace(go.Bar(x=clean_rebalance_df['date'], y=clean_rebalance_df['break_even_days'],
                         name='Break Even Days'), row=5, col=1)
    fig.add_trace(go.Bar(x=clean_rebalance_df['date'], y=clean_rebalance_df['offset_period'],
                         name='Offset Period'), row=5, col=1)
    
    # Update y-axis labels
    fig.update_yaxes(title_text="Return (%)", row=1, col=1)
    fig.update_yaxes(title_text="ETH", row=2, col=1)
    fig.update_yaxes(title_text="ETH", row=3, col=1)
    fig.update_yaxes(title_text="Swap Cost (%)", row=4, col=1)
    fig.update_yaxes(title_text="Days", row=5, col=1)
    
    # Update layout
    fig.update_layout(
        height=1600, 
        width=1000, 
        title_text="",
        plot_bgcolor='white',
        paper_bgcolor='white',
        font=dict(color='black'),
    )
    
    # Update x-axes
    fig.update_xaxes(
        title_text="Date", 
        row=5, 
        col=1,
        showgrid=True, 
        gridwidth=1, 
        gridcolor='lightgray',
        zeroline=True,
        zerolinewidth=1,
        zerolinecolor='black',
    )
    
    # Update y-axes
    fig.update_yaxes(
        showgrid=True, 
        gridwidth=1, 
        gridcolor='lightgray',
        zeroline=True,
        zerolinewidth=1,
        zerolinecolor='black',
    )
    return fig
