import pandas as pd

from multicall import Call
from v2_rebalance_dashboard.get_state_by_block import (
    sync_safe_get_raw_state_by_block,
    build_blocks_to_use,
    safe_normalize_with_bool_success,
)
from v2_rebalance_dashboard.constants import balETH_AUTOPOOL_ETH_ADDRESS
import plotly.express as px


def nav_per_share_call(name: str, autopool_vault_address: str) -> Call:
    return Call(
        autopool_vault_address,
        ["convertToAssets(uint256)(uint256)", int(1e18)],
        [(name, safe_normalize_with_bool_success)],
    )


def fetch_daily_nav_per_share_to_plot():
    blocks = build_blocks_to_use()

    calls = [
        nav_per_share_call("balETH", balETH_AUTOPOOL_ETH_ADDRESS),
        # nav_per_share_call("autoETH", main_auto_pool_vault),
    ]
    nav_per_share_df = sync_safe_get_raw_state_by_block(calls, blocks)

    fig = px.line(nav_per_share_df[["balETH"]])
    fig.update_traces(line=dict(width=4))
    fig.update_layout(
        # not attached to these settings
        title="",
        xaxis_title="",
        yaxis_title="NAV Per Share",
        title_x=0.5,
        margin=dict(l=40, r=40, t=40, b=40),
        height=600,
        width=600 * 3,
        font=dict(size=16),
        legend=dict(font=dict(size=18), orientation='h', x=0.5, xanchor='center', y=-0.2),
        legend_title_text='',
        plot_bgcolor='white',
        paper_bgcolor='white',
        xaxis=dict(showgrid=True, gridcolor='lightgray'),
        yaxis=dict(showgrid=True, gridcolor='lightgray')
    )
    return fig
