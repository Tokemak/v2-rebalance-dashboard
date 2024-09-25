import streamlit as st

# this needs to be first because otherwise we get this error:
# `StreamlitAPIException: set_page_config() can only be called once per app page,
# and must be called as the first Streamlit command in your script.`
st.set_page_config(
    page_title="Mainnet Autopool Diagnostics Dashboard",
    layout="wide",
    initial_sidebar_state="expanded",
)


from mainnet_launch.key_metrics import display_key_metrics
from mainnet_launch.weighted_crm import display_weighted_crm
from mainnet_launch.destination_allocation_over_time import display_destination_allocation_over_time
from mainnet_launch.rebalance_events import display_rebalance_events
from mainnet_launch.autopool_lp_stats import display_autopool_lp_stats

from mainnet_launch.constants import ALL_AUTOPOOLS, AUTOPOOL_NAME_TO_CONSTANTS, AutopoolConstants


def main():

    st.markdown(
        """
        <style>
        .main {
            max-width: 85%;
            margin: 0 auto;
            padding-top: 40px;
        }
        .stPlotlyChart {
            width: 100%;
            height: auto;
            min-height: 300px;
            max-height: 600px;
            background-color: #f0f2f6;
            border-radius: 5px;
            padding: 20px;
        }
        @media (max-width: 768px) {
            .stPlotlyChart {
                min-height: 250px;
                max-height: 450px;
            }
        }
        .stPlotlyChart {
            background-color: #f0f2f6;
            border-radius: 5px;
            padding: 10px;
        }
        .stExpander {
            background-color: #e6e9ef;
            border-radius: 5px;
            padding: 10px;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    st.title("Autopool Diagnostics Dashboard")

    st.sidebar.title("Navigation")
    names = [autopool.name for autopool in ALL_AUTOPOOLS]

    pool_name = st.sidebar.selectbox("Select Pool", names)
    autopool = AUTOPOOL_NAME_TO_CONSTANTS[pool_name]

    # Sidebar Pages
    page = st.sidebar.radio(
        "Go to",
        [
            "Key Metrics",
            "Autopool Exposure",
            "Allocation Over Time",
            "Weighted CRM",
            "Rebalance Events",
            "Autopool Deposits and Withdrawals",
        ],
    )

    display_autopool(autopool, page)


def display_autopool(autopool: AutopoolConstants, page: str):

    content_functions = {
        "Key Metrics": display_key_metrics,
        "Autopool Exposure": display_autopool_exposure,
        "Allocation Over Time": display_destination_allocation_over_time,
        "Weighted CRM": display_weighted_crm,
        "Rebalance Events": display_rebalance_events,
        "Autopool Deposits and Withdrawals": display_autopool_lp_stats,
    }

    # Get the function based on the page selected
    content_function = content_functions.get(page)
    if content_function:
        # Call the function with the pool name
        content_function(autopool)
    else:
        st.write("Page not found.")


def display_autopool_exposure(pool_name):
    st.write(f"Displaying Autopool Exposure for {pool_name}...")
    # Add content here


def display_allocation_over_time(pool_name):
    st.write(f"Displaying Allocation Over Time for {pool_name}...")
    # Add content here


if __name__ == "__main__":
    main()
