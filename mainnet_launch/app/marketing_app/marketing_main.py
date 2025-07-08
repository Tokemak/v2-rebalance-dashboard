"""Holds internal functions to make marketing data"""

import streamlit as st
from mainnet_launch.app.ui_config_setup import config_plotly_and_streamlit, STREAMLIT_MARKDOWN_HTML, format_timedelta

config_plotly_and_streamlit()


import pandas as pd
from mainnet_launch.constants import *
from mainnet_launch.app.marketing_app.marketing_pages.autopool_cumulative_volume import (
    fetch_and_render_cumulative_volume,
)


def dummy_page():
    st.title("dummy page")


MARKETING_CONTENT_FUNCTIONS = {"Start": dummy_page, "Cumulative USD Volume": fetch_and_render_cumulative_volume}


def main():
    st.markdown(STREAMLIT_MARKDOWN_HTML, unsafe_allow_html=True)
    st.title("Marketing Data Dashboard")
    st.sidebar.title("Navigation")

    page = st.sidebar.radio("Go to", MARKETING_CONTENT_FUNCTIONS.keys())

    if page:
        MARKETING_CONTENT_FUNCTIONS[page]()


if __name__ == "__main__":
    main()

# streamlit run mainnet_launch/app/marketing_app/marketing_main.py
