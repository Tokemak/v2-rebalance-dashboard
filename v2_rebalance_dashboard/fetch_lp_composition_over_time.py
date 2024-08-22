import pandas as pd

from multicall import Call
from v2_rebalance_dashboard.get_state_by_block import (
    sync_safe_get_raw_state_by_block,
    sync_get_raw_state_by_block_one_block,
    build_blocks_to_use,
    safe_normalize_with_bool_success,
)
import plotly.express as px

# today I am working more on the rebalance dashboard

destination_vaults_and_stats_df = pd.read_csv("vaults.csv", index_col=0)[['vaultAddress','name','stats']]


def getAssetBreakdown_call(name: str, incentive_stats: str) -> Call:
    return Call(
        incentive_stats,
        ["lastSnapshotTotalApr()(uint256)"],
        [(name, safe_normalize_with_bool_success)],
    )



# how much 

# (lp Token value at start and lp token value at end, eth value at the end of reward tokens accumlated)
# ingnore gas costs for now. swap costs, slippage, timeing, competence, routing, all of that jazz


# incentiveStats.lastSnapshotTotalApr() -> prior incnetive APR, good approx, less all the period finish issues


# function getDestinationInfo(address destVault) external view returns (AutopoolDebt.DestinationInfo memory) {
#     return _destinationInfo[destVault];


# struct DestinationInfo {
#     /// @notice Current underlying value at the destination vault
#     /// @dev Used for calculating totalDebt, mid point of min and max
#     uint256 cachedDebtValue;
#     /// @notice Current minimum underlying value at the destination vault
#     /// @dev Used for calculating totalDebt during withdrawal
#     uint256 cachedMinDebtValue;
#     /// @notice Current maximum underlying value at the destination vault
#     /// @dev Used for calculating totalDebt of the deposit
#     uint256 cachedMaxDebtValue;
#     /// @notice Last block timestamp this info was updated
#     uint256 lastReport;
#     /// @notice How many shares of the destination vault we owned at last report
#     uint256 ownedShares;
# }

# path, get all valid destinations_vaults, 
# for d in destinations:
# build call getDestinationInfo(d) // cachedDebtValue (what we think its worth in ETH) is the value that matters.
# Maybe combine them


# incentive_stats_contracts = [destination_vault.getStats() for destination_vault in vaults]

# for stats in incentive_stats_contracts:
# handle_current_stats = stats.current(for block in blocks)

# f((getDestinationSummaryStats(dest).compositeAprOut).DropIfOver100% * eth precent value) composite APR 

def handle_getAssetBreakdown(success, AssetBreakdown):
    # struct AssetBreakdown {
    #     uint256 totalIdle;
    #     uint256 totalDebt;
    #     uint256 totalDebtMin;
    #     uint256 totalDebtMax;
    # }
    if success:
        totalIdle, totalDebt, totalDebtMin, totalDebtMin = AssetBreakdown
        return int(totalIdle + totalDebt) / 1e18
    return None


def getAssetBreakdown_call(name: str, autopool_vault_address: str) -> Call:
    return Call(
        autopool_vault_address,
        ["getAssetBreakdown()((uint256,uint256,uint256,uint256))"],
        [(name, handle_getAssetBreakdown)],
    )


def fetch_daily_nav_to_plot():
    blocks = build_blocks_to_use()

    balETH_auto_pool_vault = "0x72cf6d7C85FfD73F18a83989E7BA8C1c30211b73"
    main_auto_pool_vault = "0x49C4719EaCc746b87703F964F09C22751F397BA0"

    calls = [
        getAssetBreakdown_call("balETH", balETH_auto_pool_vault),
        getAssetBreakdown_call("autoETH", main_auto_pool_vault),
    ]

    nav_df = sync_safe_get_raw_state_by_block(calls, blocks)

    fig = px.scatter(nav_df[["balETH", "autoETH"]])
    fig.update_layout(
        # not attached to these settings
        title="NAV",
        xaxis_title="Date",
        yaxis_title="Idle + Debt (ETH)",
        title_x=0.5,
        margin=dict(l=40, r=40, t=40, b=40),
        height=500,
        width=800,
    )
    return fig
