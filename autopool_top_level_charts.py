import streamlit as st
import os
import plotly.express as px
from v2_rebalance_dashboard.fetch_destination_summary_stats import fetch_summary_stats_figures
from v2_rebalance_dashboard.fetch_asset_combination_over_time import fetch_asset_composition_over_time_to_plot
from v2_rebalance_dashboard.fetch_nav_per_share import fetch_daily_nav_per_share_to_plot
from v2_rebalance_dashboard.fetch_nav import fetch_daily_nav_to_plot
from v2_rebalance_dashboard.get_rebalance_events_summary import fetch_clean_rebalance_events
from v2_rebalance_dashboard.fetch_growth_of_a_dollar import fetch_growth_of_a_dollar_figure


def get_autopool_diagnostics_plotData(autopool_name: str):
    if autopool_name != "balETH":
        raise ValueError("only works for balETH autopool")

    (
        eth_allocation_bar_chart_fig,
        composite_return_out_fig1,
        composite_return_out_fig2,
        current_allocation_pie_fig,
        uwcr_df,
    ) = fetch_summary_stats_figures()
    nav_per_share_df = fetch_daily_nav_per_share_to_plot()
    nav_fig = fetch_daily_nav_to_plot()
    asset_allocation_bar_fig, asset_allocation_pie_fig = fetch_asset_composition_over_time_to_plot()
    rebalance_fig = fetch_clean_rebalance_events()
    growth_of_a_dollar_fig = fetch_growth_of_a_dollar_figure()

    return {
        "eth_allocation_bar_chart_fig": eth_allocation_bar_chart_fig,
        "composite_return_out_fig1": composite_return_out_fig1,
        "composite_return_out_fig2": composite_return_out_fig2,
        "current_allocation_pie_fig": current_allocation_pie_fig,
        "nav_per_share_df": nav_per_share_df,
        "nav_fig": nav_fig,
        "asset_allocation_bar_fig": asset_allocation_bar_fig,
        "asset_allocation_pie_fig": asset_allocation_pie_fig,
        "rebalance_fig": rebalance_fig,
        "uwcr_df": uwcr_df,
        "growth_of_a_dollar_fig": growth_of_a_dollar_fig,
    }


def diffReturn(x: list):
    if len(x) < 2:
        return None  # Not enough elements to calculate difference
    return x[-1] - x[-2]


def show_key_metrics(plotData):
    st.header("Key Metrics")
    nav_per_share_df = plotData["nav_per_share_df"]
    uwcr_df = plotData["uwcr_df"]
    col1, col2, col3, col4 = st.columns(4)
    col1.metric(
        "30-day Return (%)",
        nav_per_share_df["30_day_annualized_return"][-1],
        diffReturn(nav_per_share_df["30_day_annualized_return"]),
    )
    col2.metric(
        "7-day Return (%)",
        nav_per_share_df["7_day_annualized_return"][-1],
        diffReturn(nav_per_share_df["7_day_annualized_return"]),
    )
    col3.metric("Expected Annual Return (%)", uwcr_df["Expected_Return"][-1], diffReturn(uwcr_df["Expected_Return"]))

    # Plot NAV Per Share
    nav_fig = px.line(nav_per_share_df, y="balETH", title=" ")
    nav_fig.update_traces(line=dict(width=3))
    nav_fig.update_layout(
        title_x=0.5,
        margin=dict(l=40, r=40, t=40, b=80),
        height=400,
        width=800,
        font=dict(size=16),
        yaxis_title="NAV Per Share",
        xaxis_title="",
        plot_bgcolor="white",
        paper_bgcolor="white",
        xaxis=dict(showgrid=True, gridcolor="lightgray"),
        yaxis=dict(showgrid=True, gridcolor="lightgray"),
    )

    # Plot 30-day Annualized Return
    annualized_return_fig = px.line(nav_per_share_df, y="30_day_annualized_return", title=" ")
    annualized_return_fig.update_traces(line=dict(width=3))
    annualized_return_fig.update_layout(
        title_x=0.5,
        margin=dict(l=40, r=40, t=40, b=80),
        height=400,
        width=800,
        font=dict(size=16),
        yaxis_title="30-day Annualized Return (%)",
        xaxis_title="",
        plot_bgcolor="white",
        paper_bgcolor="white",
        xaxis=dict(showgrid=True, gridcolor="lightgray"),
        yaxis=dict(showgrid=True, gridcolor="lightgray"),
    )

    # Plot 7-day Annualized Return
    annualized_7dreturn_fig = px.line(nav_per_share_df, y="7_day_annualized_return", title=" ")
    annualized_7dreturn_fig.update_traces(line=dict(width=3))
    annualized_7dreturn_fig.update_layout(
        title_x=0.5,
        margin=dict(l=40, r=40, t=40, b=80),
        height=400,
        width=800,
        font=dict(size=16),
        yaxis_title="7-day Annualized Return (%)",
        xaxis_title="",
        plot_bgcolor="white",
        paper_bgcolor="white",
        xaxis=dict(showgrid=True, gridcolor="lightgray"),
        yaxis=dict(showgrid=True, gridcolor="lightgray"),
    )

    # Plot unweighted CR
    uwcr_return_fig = px.line(uwcr_df, y="Expected_Return", title=" ")
    uwcr_return_fig.update_traces(line=dict(width=3))
    uwcr_return_fig.update_layout(
        title_x=0.5,
        margin=dict(l=40, r=40, t=40, b=80),
        height=400,
        width=800,
        font=dict(size=16),
        yaxis_title="Expected Annualized Return (%)",
        xaxis_title="",
        plot_bgcolor="white",
        paper_bgcolor="white",
        xaxis=dict(showgrid=True, gridcolor="lightgray"),
        yaxis=dict(showgrid=True, gridcolor="lightgray"),
    )

    # Insert gap
    st.markdown("<div style='margin: 7em 0;'></div>", unsafe_allow_html=True)

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.subheader("NAV per share")
        st.plotly_chart(nav_fig, use_container_width=True)
    with col2:
        st.subheader("NAV")
        st.plotly_chart(plotData["nav_fig"], use_container_width=True)

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.subheader("30-day Annualized Return (%)")
        st.plotly_chart(annualized_return_fig, use_container_width=True)
    with col2:
        st.subheader("7-day Annualized Return (%)")
        st.plotly_chart(annualized_7dreturn_fig, use_container_width=True)
    with col3:
        st.subheader("Expected Annualized Return (%)")
        st.plotly_chart(uwcr_return_fig, use_container_width=True)

    with st.expander("See explanation for Key Metrics"):
        st.write(
            """
        This section displays the key performance indicators for the Autopool:
        - NAV per share: The Net Asset Value per share over time.
        - NAV: The total Net Asset Value of the Autopool.
        - 30-day and 7-day Annualized Returns: Percent annual return derived from NAV per share changes. 
        - Expected Annualized Return: Projected percent annual return based on current allocations of the Autopool.
        """
        )


def show_autopool_exposure(plotData):
    st.header("Autopool Exposure")
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Autopool Destination Exposure")
        st.plotly_chart(plotData["current_allocation_pie_fig"], use_container_width=True)
    with col2:
        st.subheader("Autopool Token Exposure")
        st.plotly_chart(plotData["asset_allocation_pie_fig"], use_container_width=True)
    with st.expander("See explanation for Autopool Exposure"):
        st.write(
            """
        This section shows the current allocation of the Autopool:
        - Autopool Destination Exposure: Breakdown of allocations to different destinations.
        - Autopool Token Exposure: Distribution of underlying tokens in various destinations the Autopool is allocated to.
        """
        )


def show_allocation_over_time(plotData):
    st.header("Allocation Over Time")
    st.plotly_chart(plotData["eth_allocation_bar_chart_fig"], use_container_width=True)
    with st.expander("See explanation for Allocation Over Time"):
        st.write(
            """
        This chart displays how the Autopool's allocation has changed over time:
        - X-axis represents the date.
        - Y-axis shows the percentage allocation to different assets or destinations.
        - Colors represent different destination or yield sources.
        """
        )


def show_weighted_crm(plotData):
    st.header("Weighted CRM")
    st.plotly_chart(plotData["composite_return_out_fig1"], use_container_width=True)

    st.header("Weighted CRM with Destinations")
    st.plotly_chart(plotData["composite_return_out_fig2"], use_container_width=True)
    with st.expander("See explanation for Weighted CRM"):
        st.write(
            """
        Weighted Composite Return Model (CRM) plotData:
        - The first chart shows the overall weighted out-CRM for the Autopool.
        - The second chart breaks down the out-CRM by individual destinations along with that for the Autopool.
        """
        )


def show_rebalance_events(plotData):
    st.header("Rebalance Events")
    st.plotly_chart(plotData["rebalance_fig"], use_container_width=True)
    with st.expander("See explanation for Rebalance Events"):
        st.write(
            """
        This chart shows the history of rebalancing events:
        - Each point in time represents a rebalance event.
        - Composite Returns: In and out composite return metric for the incoming and outgoing destinations involved in the rebalance
        - In/Out ETH Values: Amount in ETH moved from outgoing to incoming destinations.
        - Swap Cost and Predicted Gain: Swapping costs in ETH associated with exchanging tokens to go from outgoing to incoming destinations.
        - Swap Cost as a Percentage of Out ETH value - Swap cost converted to % value
        - Break Even Days and Offset Period: 
                1. Break Even Days: Number of days needed make back the swap cost using only the incremental APR when going from outgoing to incoming destination
                2. Offset Period: Swap cost offset period in Days. This is used in the rebalance test conducted by the Autopool to gate-keep rebalance / swap transactions
                 
        """
        )


def show_growth_of_a_dollar(plotData):
    st.header("Growth of an ETH Over Time")
    st.plotly_chart(plotData["growth_of_a_dollar_fig"], use_container_width=True)

    with st.expander("See explanation for Growth of a Dollar"):
        st.write(
            """
            This chart displays the growth of 1 ETH invested in the balETH Autopool and other Balancer Destinations.
            
            - balETH is from navPerShare(). It is actual performace with real dollars
            
            - Other Destinations do not account for gas, slippage, swap fees, or cost to enter / exit the pools. So they are overestimates   
            
            """
        )


def main():
    st.set_page_config(
        page_title="Autopool Diagnostics Dashboard",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    # Custom CSS
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

    # Sidebar
    st.sidebar.title("Navigation")
    page = st.sidebar.radio(
        "Go to",
        [
            "Key Metrics",
            "Autopool Exposure",
            "Allocation Over Time",
            "Weighted CRM",
            "Rebalance Events",
            "Growth of a Dollar",
        ],
    )

    autopool_name = os.getenv("AUTOPOOL_NAME", "balETH")

    # Load data with progress bar
    with st.spinner("Loading data..."):
        plotData = get_autopool_diagnostics_plotData(autopool_name)
    st.success("Data loaded successfully!")

    # Main content
    if page == "Key Metrics":
        show_key_metrics(plotData)
    elif page == "Autopool Exposure":
        show_autopool_exposure(plotData)
    elif page == "Allocation Over Time":
        show_allocation_over_time(plotData)
    elif page == "Weighted CRM":
        show_weighted_crm(plotData)
    elif page == "Rebalance Events":
        show_rebalance_events(plotData)
    elif page == "Growth of a Dollar":
        show_growth_of_a_dollar(plotData)


if __name__ == "__main__":
    main()
