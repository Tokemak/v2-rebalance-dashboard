import pandas as pd

from multicall import Call
from v2_rebalance_dashboard.get_state_by_block import (
    sync_safe_get_raw_state_by_block,
    build_blocks_to_use,
)
import plotly.express as px

# not used, replaced by summary stats maybe we want this for debt in / debt out later


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

    balETH_AUTOPOOL_ETH_ADDRESS = "0x72cf6d7C85FfD73F18a83989E7BA8C1c30211b73"
    # main_auto_pool_vault = "0x49C4719EaCc746b87703F964F09C22751F397BA0"

    calls = [
        getAssetBreakdown_call("balETH", balETH_AUTOPOOL_ETH_ADDRESS),
        # getAssetBreakdown_call("autoETH", main_auto_pool_vault),
    ]

    nav_df = sync_safe_get_raw_state_by_block(calls, blocks)

    fig = px.line(nav_df[["balETH"]])
    fig.update_traces(line=dict(width=4))
    fig.update_layout(
        # not attached to these settings
        title="",
        xaxis_title="",
        yaxis_title="NAV (ETH)",
        title_x=0.5,
        margin=dict(l=40, r=40, t=40, b=40),
        height=600,
        width=600 * 3,
        font=dict(size=16),
        legend=dict(font=dict(size=18), orientation="h", x=0.5, xanchor="center", y=-0.2),
        legend_title_text="",
        plot_bgcolor="white",
        paper_bgcolor="white",
        xaxis=dict(showgrid=True, gridcolor="lightgray"),
        yaxis=dict(showgrid=True, gridcolor="lightgray"),
    )
    return fig
