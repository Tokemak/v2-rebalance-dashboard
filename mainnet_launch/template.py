import streamlit as st
import plotly.graph_objects as go


from mainnet_launch.constants import (
    CACHE_TIME,
    AutopoolConstants,
    ALL_AUTOPOOLS,
    eth_client,
    SOLVER_REBALANCE_PLANS_DIR,
    AUTO_ETH,
)


# called on page display
def fetch_data_and_render(autopool: AutopoolConstants):
    data = fetch_data(autopool)
    _render_streamlit_page(data)


def _render_streamlit_page(*args):
    # take the plots and figure and render them with streamlit
    pass


# called in loop
@st.cache_data(3600)
def fetch_data(autopool: AutopoolConstants) -> list[go.Figure]:
    # fetch all the data and make the figure
    pass
