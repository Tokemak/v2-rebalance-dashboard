import pandas as pd

from multicall import Call
from v2_rebalance_dashboard.get_state_by_block import (
    sync_safe_get_raw_state_by_block,
    build_blocks_to_use,
    safe_normalize_with_bool_success,
)
from v2_rebalance_dashboard.constants import balETH_AUTOPOOL_ETH_ADDRESS


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
    ]
    nav_per_share_df = sync_safe_get_raw_state_by_block(calls, blocks)

    # Calculate the 30-day difference and annualized return
    nav_per_share_df['30_day_difference'] = nav_per_share_df['balETH'].diff(periods=30)
    nav_per_share_df['30_day_annualized_return'] = (nav_per_share_df['30_day_difference'] * (365 / 30) * 100).dropna()

    return nav_per_share_df


