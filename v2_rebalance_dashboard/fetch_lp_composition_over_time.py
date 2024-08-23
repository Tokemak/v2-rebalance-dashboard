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

destination_df = pd.read_csv(
    "/home/parker/Documents/Tokemak/v2-rebalance-dashboard/v2_rebalance_dashboard/vaults.csv", index_col=0
)[["vaultAddress", "name", "stats"]]

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


def getDestinationInfo_call(name: str, autopool_vault_address: str, destination_vault: str):

    def handle_DestinationInfo(success, DestinationInfo):
        if success:
            cachedDebtValue, cachedMinDebtValue, cachedMaxDebtValue, lastReport, ownedShares = DestinationInfo
            return {"cachedDebtValue": cachedDebtValue / 1e18, "ownedShares": ownedShares / 1e18}

    return Call(
        autopool_vault_address,
        ["getDestinationInfo(address)((uint256,uint256,uint256,uint256,uint256))", destination_vault],
        [(name, handle_DestinationInfo)],
    )


def fetch_lp_tokens_and_eth_value_per_destination():
    blocks = build_blocks_to_use()
    balETH_auto_pool_vault = "0x72cf6d7C85FfD73F18a83989E7BA8C1c30211b73"

    calls = [
        getDestinationInfo_call(name, balETH_auto_pool_vault, vault)
        for (name, vault) in zip(destination_df["name"], destination_df["vaultAddress"])
    ]
    df = sync_safe_get_raw_state_by_block(calls, blocks)

    cachedDebtValue_df = df.map(
        lambda cell: cell["cachedDebtValue"] if isinstance(cell, dict) and "cachedDebtValue" in cell else 0
    )

    # only look at destinatios we have touched,
    # eg where at least one of the values in eth_value_in_destination is not 0
    # this is just to make the legend cleaner
    cachedDebtValue_df = cachedDebtValue_df.loc[:, (cachedDebtValue_df != 0).any(axis=0)]

    fig = px.bar(cachedDebtValue_df)
    fig.update_layout(
        # not attached to these settings
        title="ETH Value By Destination",
        xaxis_title="Date",
        yaxis_title="ETH value",
        title_x=0.5,
        margin=dict(l=40, r=40, t=40, b=40),
        height=500,
        width=800,
    )
    return fig
