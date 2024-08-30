import streamlit as st
import os
from v2_rebalance_dashboard.fetch_destination_summary_stats import fetch_summary_stats_figures
from v2_rebalance_dashboard.fetch_asset_combination_over_time import fetch_asset_composition_over_time_to_plot
from v2_rebalance_dashboard.fetch_nav_per_share import fetch_daily_nav_per_share_to_plot
from v2_rebalance_dashboard.fetch_nav import fetch_daily_nav_to_plot

def get_autopool_diagnostics_charts(autopool_name:str):
    if autopool_name != "balETH":
        raise ValueError("only works for balETH autopool")

    eth_allocation_bar_chart_fig, composite_return_out_fig1, composite_return_out_fig2, current_allocation_pie_fig = fetch_summary_stats_figures()
    nav_per_share_fig = fetch_daily_nav_per_share_to_plot()
    nav_fig = fetch_daily_nav_to_plot()
    asset_allocation_bar_fig, asset_allocation_pie_fig = fetch_asset_composition_over_time_to_plot()

    return {
        "eth_allocation_bar_chart_fig": eth_allocation_bar_chart_fig,
        "composite_return_out_fig1": composite_return_out_fig1,
        "composite_return_out_fig2": composite_return_out_fig2,
        "current_allocation_pie_fig": current_allocation_pie_fig,
        "nav_per_share_fig": nav_per_share_fig,
        "nav_fig": nav_fig,
        "asset_allocation_bar_fig": asset_allocation_bar_fig,
        "asset_allocation_pie_fig": asset_allocation_pie_fig,
    }

def main():
    # Set the page configuration
    st.set_page_config(
        page_title="Autopool Diagnostics Dashboard",
        layout="wide",  # Use a wide layout for better use of screen space
        initial_sidebar_state="expanded",
    )

    # Center-align the main title
    st.markdown(
        """
        <h1 style='text-align: center;'>
            Autopool Diagnostics Dashboard
        </h1>
        """, 
        unsafe_allow_html=True
    )

    # Get autopool_name from environment variable
    autopool_name = os.getenv('AUTOPOOL_NAME', 'balETH')

    # Call the function with the autopool_name argument
    charts = get_autopool_diagnostics_charts(autopool_name)

    # Display charts with center-aligned headings
    col1, col2 = st.columns(2)  # Use columns for side-by-side charts
    with col1:
        st.markdown(
            """
            <h3 style='text-align: center;'>
                NAV per share
            </h3>
            """, 
            unsafe_allow_html=True
        )
        st.plotly_chart(charts["nav_per_share_fig"], use_container_width=True)
    with col2:
        st.markdown(
            """
            <h3 style='text-align: center;'>
                NAV
            </h3>
            """, 
            unsafe_allow_html=True
        )
        st.plotly_chart(charts["nav_fig"], use_container_width=True)
    st.markdown("---")


    # Display charts with center-aligned headings
    col1, col2 = st.columns(2)  # Use columns for side-by-side charts
    with col1:
        st.markdown(
            """
            <h3 style='text-align: center;'>
                Autopool Destination Exposure
            </h3>
            """, 
            unsafe_allow_html=True
        )
        st.plotly_chart(charts["current_allocation_pie_fig"], use_container_width=True)
    with col2:
        st.markdown(
            """
            <h3 style='text-align: center;'>
                Autopool Token Exposure
            </h3>
            """, 
            unsafe_allow_html=True
        )
        st.plotly_chart(charts["asset_allocation_pie_fig"], use_container_width=True)
    st.markdown("---")  

    st.markdown(
        """
        <h3 style='text-align: center;'>
            Autopool Allocation Over Time
        </h3>
        """, 
        unsafe_allow_html=True
    )
    st.plotly_chart(charts["eth_allocation_bar_chart_fig"], use_container_width=True)
    st.markdown("---")

    st.markdown(
        """
        <h3 style='text-align: center;'>
            Autopool Weighted CRM
        </h3>
        """, 
        unsafe_allow_html=True
    )
    st.plotly_chart(charts["composite_return_out_fig1"], use_container_width=True)
    st.markdown("---")

    st.markdown(
        """
        <h3 style='text-align: center;'>
            Autopool Weighted CRM w/ Destinations
        </h3>
        """, 
        unsafe_allow_html=True
    )
    st.plotly_chart(charts["composite_return_out_fig2"], use_container_width=True)
    st.markdown("---")

if __name__ == "__main__":
    main()