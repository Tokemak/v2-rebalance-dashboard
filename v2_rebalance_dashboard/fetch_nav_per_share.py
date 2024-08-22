import pandas as pd

from multicall import Call
from v2_rebalance_dashboard.get_state_by_block import (
    sync_safe_get_raw_state_by_block,
    build_blocks_to_use,
    safe_normalize_with_bool_success,
)
import plotly.express as px


def nav_per_share_call(name: str, autopool_vault_address: str) -> Call:
    return Call(
        autopool_vault_address,
        ["convertToAssets(uint256)(uint256)", int(1e18)],
        [(name, safe_normalize_with_bool_success)],
    )


def fetch_daily_nav_per_share_to_plot():
    blocks = build_blocks_to_use()

    balETH_auto_pool_vault = "0x72cf6d7C85FfD73F18a83989E7BA8C1c30211b73"
    main_auto_pool_vault = "0x49C4719EaCc746b87703F964F09C22751F397BA0"

    calls = [
        nav_per_share_call("balETH", balETH_auto_pool_vault),
        # nav_per_share_call("autoETH", main_auto_pool_vault),
    ]
    nav_per_share_df = sync_safe_get_raw_state_by_block(calls, blocks)

    fig = px.scatter(nav_per_share_df[["balETH", "autoETH"]])
    fig.update_layout(
        # not attached to these settings
        title="navPerShare",
        xaxis_title="Date",
        yaxis_title="NavPerShare",
        title_x=0.5,
        margin=dict(l=40, r=40, t=40, b=40),
        height=500,
        width=800,
    )
    return fig
