import streamlit as st

from mainnet_launch.key_metrics import display_key_metrics


def main():
    st.set_page_config(
        page_title="Mainnet Autopool Diagnostics Dashboard",
        layout="wide",
        initial_sidebar_state="expanded",
    )

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

    selected_pool = st.sidebar.selectbox("Select Pool", ("autoETH", "autoLRT", "balETH"))

    # Sidebar Pages
    page = st.sidebar.radio(
        "Go to",
        [
            "Key Metrics",
            "Autopool Exposure",
            "Allocation Over Time",
            "Weighted CRM",
            "Rebalance Events",
        ],
    )

    # Display content based on selected pool and page
    display_pool(selected_pool, page)


def display_pool(pool_name, page):
    st.subheader(f"{pool_name}")

    content_functions = {
        "Key Metrics": display_key_metrics,
        "Autopool Exposure": display_autopool_exposure,
        "Allocation Over Time": display_allocation_over_time,
        "Weighted CRM": display_weighted_crm,
        "Rebalance Events": display_rebalance_events,
    }

    # Get the function based on the page selected
    content_function = content_functions.get(page)
    if content_function:
        # Call the function with the pool name
        content_function(pool_name)
    else:
        st.write("Page not found.")


# def display_key_metrics(pool_name):
#     st.write(f"Displaying Key Metrics for {pool_name}...")
#     # Add your charts, metrics, or tables here


def display_autopool_exposure(pool_name):
    st.write(f"Displaying Autopool Exposure for {pool_name}...")
    # Add content here


def display_allocation_over_time(pool_name):
    st.write(f"Displaying Allocation Over Time for {pool_name}...")
    # Add content here


def display_weighted_crm(pool_name):
    st.write(f"Displaying Weighted CRM for {pool_name}...")
    # Add content here


def display_rebalance_events(pool_name):
    st.write(f"Displaying Rebalance Events for {pool_name}...")
    # Add content here


if __name__ == "__main__":
    main()
