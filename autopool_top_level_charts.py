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

    eth_allocation_bar_chart_fig, composite_return_out_fig1, composite_return_out_fig2, current_allocation_pie_fig = fetch_summary_stats_figures()
    nav_per_share_fig, return_fig = fetch_daily_nav_per_share_to_plot()
    nav_fig = fetch_daily_nav_to_plot()
    asset_allocation_bar_fig, asset_allocation_pie_fig = fetch_asset_composition_over_time_to_plot()
    rebalance_fig = fetch_clean_rebalance_events()

    return {
        "eth_allocation_bar_chart_fig": eth_allocation_bar_chart_fig,
        "composite_return_out_fig1": composite_return_out_fig1,
        "composite_return_out_fig2": composite_return_out_fig2,
        "current_allocation_pie_fig": current_allocation_pie_fig,
        "nav_per_share_fig": nav_per_share_fig,
        "return_fig": return_fig,
        "nav_fig": nav_fig,
        "asset_allocation_bar_fig": asset_allocation_bar_fig,
        "asset_allocation_pie_fig": asset_allocation_pie_fig,
        "rebalance_fig": rebalance_fig
    }

def main():
    st.set_page_config(
        page_title="Autopool Diagnostics Dashboard",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    # Custom CSS for centering and width
    st.markdown(
        """
        <style>
        .main {
            max-width: 75%;
            margin: 0 auto;
            padding-top: 40px;
        }
        </style>
        """,
        unsafe_allow_html=True
    )

    st.markdown(
        """
        <h1 style='text-align: center;'>
            Autopool Diagnostics Dashboard
        </h1>
        """, 
        unsafe_allow_html=True
    )

    # Add index
    st.markdown("""
    ## Index
    - [Key Metrics](#key-metrics)
    - [Autopool Exposure](#autopool-exposure)
    - [Allocation Over Time](#allocation-over-time)
    - [Weighted CRM](#weighted-crm)
    - [Weighted CRM with Destinations](#weighted-crm-with-destinations)
    - [Rebalance Events](#rebalance-events)
    """)

    autopool_name = os.getenv('AUTOPOOL_NAME', 'balETH')
    charts = get_autopool_diagnostics_charts(autopool_name)

    st.markdown("<a name='key-metrics'></a>", unsafe_allow_html=True)
    st.markdown("## Key Metrics")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.markdown("### NAV per share")
        st.plotly_chart(charts["nav_per_share_fig"], use_container_width=True)
    with col2:
        st.markdown("### 30-day Annualized Return (%)")
        st.plotly_chart(charts["return_fig"], use_container_width=True)
    with col3:
        st.markdown("### NAV")
        st.plotly_chart(charts["nav_fig"], use_container_width=True)

    st.markdown("<a name='autopool-exposure'></a>", unsafe_allow_html=True)
    st.markdown("## Autopool Exposure")
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("### Autopool Destination Exposure")
        st.plotly_chart(charts["current_allocation_pie_fig"], use_container_width=True)
    with col2:
        st.markdown("### Autopool Token Exposure")
        st.plotly_chart(charts["asset_allocation_pie_fig"], use_container_width=True)

    st.markdown("<a name='allocation-over-time'></a>", unsafe_allow_html=True)
    st.markdown("## Allocation Over Time")
    st.plotly_chart(charts["eth_allocation_bar_chart_fig"], use_container_width=True)

    st.markdown("<a name='weighted-crm'></a>", unsafe_allow_html=True)
    st.markdown("## Weighted CRM")
    st.plotly_chart(charts["composite_return_out_fig1"], use_container_width=True)

    st.markdown("<a name='weighted-crm-with-destinations'></a>", unsafe_allow_html=True)
    st.markdown("## Weighted (out)CRM with Destinations")
    st.plotly_chart(charts["composite_return_out_fig2"], use_container_width=True)

    st.markdown("<a name='rebalance-events'></a>", unsafe_allow_html=True)
    st.markdown("## Rebalance Events")
    st.plotly_chart(charts["rebalance_fig"], use_container_width=True)

if __name__ == "__main__":
    main()