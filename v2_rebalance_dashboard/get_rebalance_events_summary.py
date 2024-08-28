from v2_rebalance_dashboard.get_events import fetch_events
from v2_rebalance_dashboard.constants import balETH_AUTOPOOL_ETH_STRATEGY_ADDRESS, eth_client
import pandas as pd
import json

with open("/home/parker/Documents/Tokemak/v2-rebalance-dashboard/v2_rebalance_dashboard/vault_abi.json", "r") as fin:
    autopool_eth_vault_abi = json.load(fin)

with open("/home/parker/Documents/Tokemak/v2-rebalance-dashboard/v2_rebalance_dashboard/strategy_abi.json", "r") as fin:
    eth_strategy_abi = json.load(fin)

destination_df = pd.read_csv(
    "/home/parker/Documents/Tokemak/v2-rebalance-dashboard/v2_rebalance_dashboard/vaults.csv", index_col=0
)
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
    in_destination = destination_vault_to_name[str.lower(row["inSummaryStats"][0])]
    out_destination = destination_vault_to_name[str.lower(row["outSummaryStats"][0])]

    out_compositeReturn = 100 * row["outSummaryStats"][9] / 1e18
    in_compositeReturn = 100 * row["inSummaryStats"][9] / 1e18
    apr_delta = in_compositeReturn - out_compositeReturn
    inEthValue = row["valueStats"][2] / 1e18
    outEthValue = row["valueStats"][3] / 1e18

    predicted_increase_after_swap_cost = predicted_gain_during_swap_cost_off_set_period - swapCost
    date = pd.to_datetime(eth_client.eth.get_block(row["block"]).timestamp, unit="s").date()

    break_even_days = None  # add later

    # first_line = slope ,out_compositeReturn, start point (days, eth value) (0, outEthValue)
    # second line = slope ,in_compositeReturn, start point (days, eth value) (0, inEthValue)

    return {
        "date": date,
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
    }


def fetch_clean_rebalance_events():
    strategy_contract = eth_client.eth.contract(balETH_AUTOPOOL_ETH_STRATEGY_ADDRESS, abi=eth_strategy_abi)
    rebalance_events = fetch_events(strategy_contract.events.RebalanceBetweenDestinations)
    clean_rebalance_df = pd.DataFrame.from_records(
        rebalance_events.apply(lambda row: make_rebalance_human_readable(row), axis=1)
    )
    return clean_rebalance_df


fetch_clean_rebalance_events()
