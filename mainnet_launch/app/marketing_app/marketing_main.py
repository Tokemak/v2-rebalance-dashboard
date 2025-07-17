"""Holds internal functions to make marketing data"""

import streamlit as st
from mainnet_launch.app.ui_config_setup import config_plotly_and_streamlit, STREAMLIT_MARKDOWN_HTML

config_plotly_and_streamlit()


from mainnet_launch.constants import *
from mainnet_launch.app.marketing_app.marketing_pages.autopool_cumulative_volume import (
    fetch_and_render_cumulative_volume,
)

from mainnet_launch.app.marketing_app.marketing_pages.apr_and_tvl_by_destination_script import (
    fetch_and_render_autopool_apy_and_allocation_over_time,
)

MARKETING_CONTENT_FUNCTIONS = {
    "Download APY and Allocation Data": fetch_and_render_autopool_apy_and_allocation_over_time,
    "All Autopools Cumulative USD Volume": fetch_and_render_cumulative_volume,
}


def main():
    st.markdown(STREAMLIT_MARKDOWN_HTML, unsafe_allow_html=True)
    st.title("Marketing Data Dashboard")
    st.sidebar.title("Navigation")

    page = st.sidebar.radio("Go to", MARKETING_CONTENT_FUNCTIONS.keys())

    body = st.empty()
    # clear any previous content, then render selected page
    # gets rid of ghost content when switching pages
    body.empty()
    with body.container():
        MARKETING_CONTENT_FUNCTIONS[page]()


if __name__ == "__main__":
    main()

# streamlit run mainnet_launch/app/marketing_app/marketing_main.py