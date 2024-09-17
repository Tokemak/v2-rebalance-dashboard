import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import streamlit as st

from mainnet_launch.constants import AutopoolConstants, time_decorator
from mainnet_launch.get_state_by_block import build_blocks_to_use

from mainnet_launch.fetch_destination_summary_stats import fetch_destination_summary_stats
from mainnet_launch.destinations import get_current_destinations_to_symbol


@st.cache_data(ttl=12 * 3600)  # 12 hours
def fetch_weighted_crm_data(autopool: AutopoolConstants) -> dict[str, pd.DataFrame]:
    blocks = build_blocks_to_use()
    uwcr_df, allocation_df, compositeReturn_out_df, total_nav_df = fetch_destination_summary_stats(blocks, autopool)
    destination_to_symbol = get_current_destinations_to_symbol(max(blocks))
    # overwrite the destination address with the destination symbol
    uwcr_df.columns = [destination_to_symbol[c] if c in destination_to_symbol else c for c in uwcr_df.columns]
    allocation_df.columns = [
        destination_to_symbol[c] if c in destination_to_symbol else c for c in allocation_df.columns
    ]
    compositeReturn_out_df.columns = [
        destination_to_symbol[c] if c in destination_to_symbol else c for c in compositeReturn_out_df.columns
    ]

    key_metric_data = {
        "uwcr_df": uwcr_df,
        "allocation_df": allocation_df,
        "compositeReturn_out_df": compositeReturn_out_df,
    }
    return key_metric_data


if __name__ == "__main__":
    from mainnet_launch.constants import ALL_AUTOPOOLS

    for a in ALL_AUTOPOOLS:
        data = fetch_weighted_crm_data(a)
        for k, df in data.items():
            print(k, a.name)
            print(df.tail())

        pass
