"""Holds internal functions to make marketing data"""

from mainnet_launch.app.ui_config_setup import config_plotly_and_streamlit, STREAMLIT_MARKDOWN_HTML

config_plotly_and_streamlit()


import streamlit as st

from mainnet_launch.constants import *
from mainnet_launch.app.marketing_app.marketing_pages import (
    MARKETING_PAGES_WITH_AUTOPOOL_ARG,
    MARKETING_PAGES_WITH_NO_ARGS,
)


CATEGORY_PROTOCOL = "Protocol-wide"
CATEGORY_AUTOPOOL = "Autopool"


def main():
    st.markdown(STREAMLIT_MARKDOWN_HTML, unsafe_allow_html=True)
    st.title("Marketing Data Dashboard")
    category = st.sidebar.radio(
        "Page type",
        [
            CATEGORY_AUTOPOOL,
            CATEGORY_PROTOCOL,
        ],
        index=0,
    )

    if category == CATEGORY_PROTOCOL:
        selected_page = st.sidebar.radio("Protocol-wide", list(MARKETING_PAGES_WITH_AUTOPOOL_ARG.keys()))
    elif category == CATEGORY_AUTOPOOL:
        selected_page = st.sidebar.radio("Autopool Pages", list(MARKETING_PAGES_WITH_NO_ARGS.keys()))
        chosen_name = st.sidebar.radio("Select Autopool", [a.name for a in ALL_AUTOPOOLS])
        selected_autopool = {a.name: a for a in ALL_AUTOPOOLS}[chosen_name]

    if selected_page:
        if category == CATEGORY_PROTOCOL:
            MARKETING_PAGES_WITH_NO_ARGS[selected_page]()
        elif category == CATEGORY_AUTOPOOL:
            MARKETING_PAGES_WITH_AUTOPOOL_ARG[selected_page](selected_autopool)


if __name__ == "__main__":
    main()
