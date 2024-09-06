import streamlit as st
import os
from v2_rebalance_dashboard.fetch_destination_summary_stats import fetch_summary_stats_figures
from v2_rebalance_dashboard.fetch_asset_combination_over_time import fetch_asset_composition_over_time_to_plot
from v2_rebalance_dashboard.fetch_nav_per_share import fetch_daily_nav_per_share_to_plot
from v2_rebalance_dashboard.fetch_nav import fetch_daily_nav_to_plot
from v2_rebalance_dashboard.get_rebalance_events_summary import fetch_clean_rebalance_events

def get_autopool_diagnostics_charts(autopool_name:str):
    if autopool_name != "balETH":
        raise ValueError("only works for balETH autopool")

    eth_allocation_bar_chart_fig, composite_return_out_fig1, composite_return_out_fig2, current_allocation_pie_fig, uw_cr_return_fig = fetch_summary_stats_figures()
    nav_per_share_fig, return30d_fig, return7d_fig = fetch_daily_nav_per_share_to_plot()
    nav_fig = fetch_daily_nav_to_plot()
    asset_allocation_bar_fig, asset_allocation_pie_fig = fetch_asset_composition_over_time_to_plot()
    rebalance_fig = fetch_clean_rebalance_events()

    return {
        "eth_allocation_bar_chart_fig": eth_allocation_bar_chart_fig,
        "composite_return_out_fig1": composite_return_out_fig1,
        "composite_return_out_fig2": composite_return_out_fig2,
        "current_allocation_pie_fig": current_allocation_pie_fig,
        "nav_per_share_fig": nav_per_share_fig,
        "return_fig": return30d_fig,
        "return7d_fig": return7d_fig,
        "nav_fig": nav_fig,
        "asset_allocation_bar_fig": asset_allocation_bar_fig,
        "asset_allocation_pie_fig": asset_allocation_pie_fig,
        "rebalance_fig": rebalance_fig,
        "uw_cr_return_fig": uw_cr_return_fig
    }

def show_key_metrics(charts):
    st.header("Key Metrics")
    col1, col2, col3 = st.columns(3)
    col1.metric("30-day Return", "9.97%", "0.37%")
    col2.metric("7-day Return", "10.23%", "2.52%")
    col3.metric("Expected Annual Return", "11.36%", "1.94%")

    col1, col2, col3 = st.columns(3)
    with col1:
        st.subheader("NAV per share")
        st.plotly_chart(charts["nav_per_share_fig"], use_container_width=True)
    with col2:
        st.subheader("NAV")
        st.plotly_chart(charts["nav_fig"], use_container_width=True)

    col1, col2, col3 = st.columns(3)
    with col1:
        st.subheader("30-day Annualized Return (%)")
        st.plotly_chart(charts["return_fig"], use_container_width=True)
    with col2:
        st.subheader("7-day Annualized Return (%)")
        st.plotly_chart(charts["return7d_fig"], use_container_width=True)
    with col3:
        st.subheader("Expected Annualized Return (%)")
        st.plotly_chart(charts["uw_cr_return_fig"], use_container_width=True)
    
    with st.expander("See explanation for Key Metrics"):
        st.write("""
        This section displays the key performance indicators for the Autopool:
        - NAV per share: The Net Asset Value per share over time.
        - NAV: The total Net Asset Value of the Autopool.
        - 30-day and 7-day Annualized Returns: Percent annual return derived from NAV per share changes. 
        - Expected Annualized Return: Projected percent annual return based on current allocations of the Autopool.
        """)

def show_autopool_exposure(charts):
    st.header("Autopool Exposure")
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Autopool Destination Exposure")
        st.plotly_chart(charts["current_allocation_pie_fig"], use_container_width=True)
    with col2:
        st.subheader("Autopool Token Exposure")
        st.plotly_chart(charts["asset_allocation_pie_fig"], use_container_width=True)
    with st.expander("See explanation for Autopool Exposure"):
        st.write("""
        This section shows the current allocation of the Autopool:
        - Autopool Destination Exposure: Breakdown of allocations to different destinations.
        - Autopool Token Exposure: Distribution of underlying tokens in various destinations the Autopool is allocated to.
        """)
def show_allocation_over_time(charts):
    st.header("Allocation Over Time")
    st.plotly_chart(charts["eth_allocation_bar_chart_fig"], use_container_width=True)
    with st.expander("See explanation for Allocation Over Time"):
        st.write("""
        This chart displays how the Autopool's allocation has changed over time:
        - X-axis represents the date.
        - Y-axis shows the percentage allocation to different assets or destinations.
        - Colors represent different destination or yield sources.
        """)

def show_weighted_crm(charts):
    st.header("Weighted CRM")
    st.plotly_chart(charts["composite_return_out_fig1"], use_container_width=True)
    
    st.header("Weighted CRM with Destinations")
    st.plotly_chart(charts["composite_return_out_fig2"], use_container_width=True)
    with st.expander("See explanation for Weighted CRM"):
        st.write("""
        Weighted Composite Return Model (CRM) charts:
        - The first chart shows the overall weighted out-CRM for the Autopool.
        - The second chart breaks down the out-CRM by individual destinations along with that for the Autopool.
        """)

def show_rebalance_events(charts):
    st.header("Rebalance Events")
    st.plotly_chart(charts["rebalance_fig"], use_container_width=True)
    with st.expander("See explanation for Rebalance Events"):
        st.write("""
        This chart shows the history of rebalancing events:
        - Each point in time represents a rebalance event.
        - Composite Returns: In and out composite return metric for the incoming and outgoing destinations involved in the rebalance
        - In/Out ETH Values: Amount in ETH moved from outgoing to incoming destinations.
        - Swap Cost and Predicted Gain: Swapping costs in ETH associated with exchanging tokens to go from outgoing to incoming destinations.
        - Swap Cost as a Percentage of Out ETH value - Swap cost converted to % value
        - Break Even Days and Offset Period: 
                1. Break Even Days: Number of days needed make back the swap cost using only the incremental APR when going from outgoing to incoming destination
                2. Offset Period: Swap cost offset period in Days. This is used in the rebalance test conducted by the Autopool to gate-keep rebalance / swap transactions
                 
        """)

def main():
    st.set_page_config(
        page_title="Autopool Diagnostics Dashboard",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    # Custom CSS
    st.markdown("""
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
    """, unsafe_allow_html=True)

    st.title("Autopool Diagnostics Dashboard")

    # Sidebar
    st.sidebar.title("Navigation")
    page = st.sidebar.radio("Go to", ["Key Metrics", "Autopool Exposure", "Allocation Over Time", "Weighted CRM", "Rebalance Events"])

    autopool_name = os.getenv('AUTOPOOL_NAME', 'balETH')

    # Load data with progress bar
    with st.spinner("Loading data..."):
        charts = get_autopool_diagnostics_charts(autopool_name)
    st.success("Data loaded successfully!")

    # Main content
    if page == "Key Metrics":
        show_key_metrics(charts)
    elif page == "Autopool Exposure":
        show_autopool_exposure(charts)
    elif page == "Allocation Over Time":
        show_allocation_over_time(charts)
    elif page == "Weighted CRM":
        show_weighted_crm(charts)
    elif page == "Rebalance Events":
        show_rebalance_events(charts)


if __name__ == "__main__":
    main()