import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import streamlit as st

from mainnet_launch.constants import AutopoolConstants
from mainnet_launch.get_state_by_block import build_blocks_to_use

from mainnet_launch.fetch_nav_per_share import fetch_nav_per_share
from mainnet_launch.fetch_destination_summary_stats import fetch_destination_summary_stats


def fetch_key_metric_data(autopool: AutopoolConstants) -> dict[str, pd.DataFrame]:
    blocks = build_blocks_to_use()
    nav_per_share_df = fetch_nav_per_share(blocks, autopool)
    uwcr_df, allocation_df, compositeReturn_df, total_nav_df = fetch_destination_summary_stats(blocks, autopool)
    key_metric_data = {
        "nav_per_share_df": nav_per_share_df,
        "uwcr_df": uwcr_df,
        "allocation_df": allocation_df,
        "compositeReturn_df": compositeReturn_df,
        "total_nav_df": total_nav_df,
    }

    return key_metric_data


if __name__ == "__main__":
    from mainnet_launch.constants import ALL_AUTOPOOLS

    fetch_key_metric_data(ALL_AUTOPOOLS[0])
