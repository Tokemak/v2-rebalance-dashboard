import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import streamlit as st

from mainnet_launch.constants import AutopoolConstants, time_decorator
from mainnet_launch.fetch_nav_per_share import fetch_nav_per_share
from mainnet_launch.get_state_by_block import build_blocks_to_use





def fetch_key_metric_data(autopool:AutopoolConstants):
    blocks = build_blocks_to_use()
    nav_per_share_df = fetch_nav_per_share(blocks, autopool)
    return nav_per_share_df
    

if __name__ == '__main__':
    
    from mainnet_launch.constants import BAL_ETH, AUTO_ETH, AUTO_LRT
    for a in [BAL_ETH, AUTO_ETH, AUTO_LRT]:
        
        data = fetch_key_metric_data(a)
        pass