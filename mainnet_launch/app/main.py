


import streamlit as st


from mainnet_launch.app.ui_config_setup import config_plotly_and_streamlit, STREAMLIT_MARKDOWN_HTML
from mainnet_launch.constants import ALL_AUTOPOOLS
from mainnet_launch.pages.page_functions import (
    AUTOPOOL_CONTENT_FUNCTIONS,
    PROTOCOL_CONTENT_FUNCTIONS,
    RISK_METRICS_FUNCTIONS,
)

CATEGORY_PROTOCOL = "Protocol-wide"
CATEGORY_RISK = "Risk Metrics"
CATEGORY_AUTOPOOL = "Autopool"


def main():

    config_plotly_and_streamlit()
    # update_if_needed_on_streamlit_server() # broken, takes too long for streamlit server, SSL connection dropped

    st.markdown(STREAMLIT_MARKDOWN_HTML, unsafe_allow_html=True)
    st.title("Autopool Diagnostics Dashboard")

    category = st.sidebar.radio(
        "Page type",
        [
            CATEGORY_AUTOPOOL,
            CATEGORY_RISK,
            CATEGORY_PROTOCOL,
        ],
        index=0,
    )

    selected_page = None
    selected_autopool = None

    if category == CATEGORY_PROTOCOL:
        selected_page = st.sidebar.radio("Protocol-wide", list(PROTOCOL_CONTENT_FUNCTIONS.keys()))
    elif category == CATEGORY_RISK:
        selected_page = st.sidebar.radio("Risk Metrics", list(RISK_METRICS_FUNCTIONS.keys()))
    elif category == CATEGORY_AUTOPOOL:
        selected_page = st.sidebar.radio("Autopool Pages", list(AUTOPOOL_CONTENT_FUNCTIONS.keys()))
        chosen_name = st.sidebar.radio("Select Autopool", [a.name for a in ALL_AUTOPOOLS])
        selected_autopool = {a.name: a for a in ALL_AUTOPOOLS}[chosen_name]

    if selected_page:
        if category == CATEGORY_PROTOCOL:
            PROTOCOL_CONTENT_FUNCTIONS[selected_page]()
        elif category == CATEGORY_RISK:
            RISK_METRICS_FUNCTIONS[selected_page]()
        elif category == CATEGORY_AUTOPOOL:
            AUTOPOOL_CONTENT_FUNCTIONS[selected_page](selected_autopool)


if __name__ == "__main__":
    main()
