import streamlit as st
import os
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import numpy as np
import sqlite3
from datetime import timedelta
from plotly.subplots import make_subplots
from v2_rebalance_dashboard.fetch_destination_summary_stats import fetch_summary_stats_figures
from v2_rebalance_dashboard.fetch_asset_combination_over_time import fetch_asset_composition_over_time_to_plot
from v2_rebalance_dashboard.fetch_nav_per_share import fetch_daily_nav_per_share_to_plot
from v2_rebalance_dashboard.fetch_nav import fetch_daily_nav_to_plot
from v2_rebalance_dashboard.get_rebalance_events_summary import fetch_clean_rebalance_events


def get_autopool_diagnostics_charts(autopool_name:str):
    if autopool_name != "balETH":
        raise ValueError("only works for balETH autopool")

    summary_stats_df = fetch_summary_stats_figures()
    nav_per_share_df = fetch_daily_nav_per_share_to_plot()
    nav_df = fetch_daily_nav_to_plot()
    pie_data, asset_df = fetch_asset_composition_over_time_to_plot()
    clean_rebalance_df = fetch_clean_rebalance_events()

    # Plot NAV per share plots
    nav_per_share_fig = px.line(nav_per_share_df, y='balETH', title='')
    nav_per_share_fig.update_traces(line=dict(width=3))
    nav_per_share_fig.update_layout(
        title_x=0.5,
        margin=dict(l=40, r=40, t=40, b=80),
        height=400,
        width=800,
        font=dict(size=16),
        yaxis_title='NAV Per Share',
        xaxis_title='',
        plot_bgcolor='white',
        paper_bgcolor='white',
        xaxis=dict(showgrid=True, gridcolor='lightgray'),
        yaxis=dict(showgrid=True, gridcolor='lightgray')
    )

    # Plot 30-day Annualized Return
    annualized_return_fig = px.line(nav_per_share_df, y='30_day_annualized_return', title='')
    annualized_return_fig.update_traces(line=dict(width=3))
    annualized_return_fig.update_layout(
        title_x=0.5,
        margin=dict(l=40, r=40, t=40, b=80),
        height=400,
        width=800,
        font=dict(size=16),
        yaxis_title='30-day Annualized Return (%)',
        xaxis_title='',
        plot_bgcolor='white',
        paper_bgcolor='white',
        xaxis=dict(showgrid=True, gridcolor='lightgray'),
        yaxis=dict(showgrid=True, gridcolor='lightgray')
    )

    # Plot NAV
    nav_fig = px.line(nav_df["balETH"])
    nav_fig.update_traces(line=dict(width=3))
    nav_fig.update_layout(
        # not attached to these settings
        title="",
        xaxis_title="",
        yaxis_title="NAV (ETH)",
        title_x=0.5,
        margin=dict(l=40, r=40, t=40, b=40),
        height=400,
        width=600,
        legend_title_text='',
        font=dict(size=16),
        plot_bgcolor='white',
        paper_bgcolor='white',
        xaxis=dict(showgrid=True, gridcolor='lightgray'),
        yaxis=dict(showgrid=True, gridcolor='lightgray')
    )

    # Rebalance Plots
    # Create subplots
    reb_fig = make_subplots(rows=5, cols=1, shared_xaxes=True, vertical_spacing=0.08,
                        subplot_titles=("Composite Returns", "in/out ETH Values",
                                        "Swap Cost and Predicted Gain", 
                                        "Swap Cost as Percentage of Out ETH Value", 
                                        "Break Even Days and Offset Period"))
    
    # Plot 1: out_compositeReturn & in_compositeReturn
    reb_fig.add_trace(go.Bar(x=clean_rebalance_df['date'], y=clean_rebalance_df['out_compositeReturn'],
                         name='Out Composite Return'), row=1, col=1)
    reb_fig.add_trace(go.Bar(x=clean_rebalance_df['date'], y=clean_rebalance_df['in_compositeReturn'],
                         name='In Composite Return'), row=1, col=1)
    
    # Plot 2: predicted_gain_during_swap_cost_offset_period, swapCost, outEthValue, inEthValue
    reb_fig.add_trace(go.Bar(x=clean_rebalance_df['date'], y=clean_rebalance_df['outEthValue'],
                         name='Out ETH Value'), row=2, col=1)
    reb_fig.add_trace(go.Bar(x=clean_rebalance_df['date'], y=clean_rebalance_df['inEthValue'],
                         name='In ETH Value'), row=2, col=1)
    reb_fig.add_trace(go.Bar(x=clean_rebalance_df['date'], y=clean_rebalance_df['predicted_gain_during_swap_cost_off_set_period'],
                         name='Predicted Gain'), row=3, col=1)
    reb_fig.add_trace(go.Bar(x=clean_rebalance_df['date'], y=clean_rebalance_df['swapCost'],
                         name='Swap Cost'), row=3, col=1)
 
    
    # Plot 3: swapCost / outETH * 100
    swap_cost_percentage = (clean_rebalance_df['slippage']) * 100
    reb_fig.add_trace(go.Bar(x=clean_rebalance_df['date'], y=swap_cost_percentage,
                         name='Swap Cost Percentage'), row=4, col=1)
    
    # Plot 4: break_even_days and offset_period
    reb_fig.add_trace(go.Bar(x=clean_rebalance_df['date'], y=clean_rebalance_df['break_even_days'],
                         name='Break Even Days'), row=5, col=1)
    reb_fig.add_trace(go.Bar(x=clean_rebalance_df['date'], y=clean_rebalance_df['offset_period'],
                         name='Offset Period'), row=5, col=1)
    
    # Update y-axis labels
    reb_fig.update_yaxes(title_text="Return (%)", row=1, col=1)
    reb_fig.update_yaxes(title_text="ETH", row=2, col=1)
    reb_fig.update_yaxes(title_text="ETH", row=3, col=1)
    reb_fig.update_yaxes(title_text="Swap Cost (%)", row=4, col=1)
    reb_fig.update_yaxes(title_text="Days", row=5, col=1)
    
    # Update layout
    reb_fig.update_layout(
        height=1600, 
        width=1000, 
        title_text="",
        plot_bgcolor='white',
        paper_bgcolor='white',
        font=dict(color='black'),
    )
    
    # Update x-axes
    reb_fig.update_xaxes(
        title_text="Date", 
        row=5, 
        col=1,
        showgrid=True, 
        gridwidth=1, 
        gridcolor='lightgray',
        zeroline=True,
        zerolinewidth=1,
        zerolinecolor='black',
    )
    
    # Update y-axes
    reb_fig.update_yaxes(
        showgrid=True, 
        gridwidth=1, 
        gridcolor='lightgray',
        zeroline=True,
        zerolinewidth=1,
        zerolinecolor='black',
    )

    # Token/Asset plots
    # pie chart
    asset_allocation_pie_fig = px.pie(
        pie_data,
        names='Asset',
        values='ETH Value',
        title='',
        color_discrete_sequence=px.colors.qualitative.Pastel
    )
    asset_allocation_pie_fig.update_layout(
        title_x=0.5,
        margin=dict(l=40, r=40, t=40, b=40),
        height=400,
        width=800,
        font=dict(size=16),
        legend=dict(font=dict(size=18), orientation='h', x=0.5, xanchor='center'),
        legend_title_text='',
        plot_bgcolor='white',
        paper_bgcolor='white'
    )
    asset_allocation_pie_fig.update_traces(textinfo='percent+label', hoverinfo='label+value+percent')



    #  area chart for token exposure over time
    asset_allocation_area_fig = px.bar(
        asset_df,
        title='',
        labels={'timestamp': '', 'value': 'Exposure Proportion'},
        color_discrete_sequence=px.colors.qualitative.Set1
    )

    asset_allocation_area_fig.update_layout(
        title_x=0.5,
        margin=dict(l=40, r=40, t=40, b=40),
        height=400,
        width=800,
        font=dict(size=16),
        xaxis_title='',
        yaxis_title='Proportion of Total Exposure',
        yaxis=dict(showgrid=True, gridcolor='lightgray'),
        plot_bgcolor='white',
        paper_bgcolor='white'
    )

    # Plot CRM plots
    eth_allocation_bar_chart_fig, composite_return_out_fig1, composite_return_out_fig2, current_allocation_pie_fig = _summary_stats_df_to_figures(summary_stats_df)

    return {
        "eth_allocation_bar_chart_fig": eth_allocation_bar_chart_fig,
        "composite_return_out_fig1": composite_return_out_fig1,
        "composite_return_out_fig2": composite_return_out_fig2,
        "current_allocation_pie_fig": current_allocation_pie_fig,
        "nav_per_share_fig": nav_per_share_fig,
        "return_fig": annualized_return_fig,
        "nav_fig": nav_fig,
        "asset_allocation_area_fig": asset_allocation_area_fig,
        "asset_allocation_pie_fig": asset_allocation_pie_fig,
        "rebalance_fig": reb_fig
    }


def _summary_stats_df_to_figures(summary_stats_df: pd.DataFrame):
    # Extract and process data
    pricePerShare_df = summary_stats_df.map(lambda row: row["pricePerShare"] if isinstance(row, dict) else None).astype(float)
    ownedShares_df = summary_stats_df.map(lambda row: row["ownedShares"] if isinstance(row, dict) else None).astype(float)
    compositeReturn_df = summary_stats_df.map(lambda row: row["compositeReturn"] if isinstance(row, dict) else None).astype(float)
    compositeReturn_df = 100 * (compositeReturn_df.clip(upper=1).replace(1, np.nan).astype(float))
    allocation_df = pricePerShare_df * ownedShares_df

    # Flatten and process the DataFrame
    allocation_df = summary_stats_df.apply(lambda row: row['pricePerShare'] * row['ownedShares'], axis=1)
    compositeReturn_df = summary_stats_df.apply(lambda row: row['compositeReturn'], axis=1)
    
    # Combine into a single DataFrame
    combined_df = pd.DataFrame({
        'date': summary_stats_df.index,
        'allocation': allocation_df,
        'composite_return': compositeReturn_df
    })


    # Limit to the last 90 days
    end_date = allocation_df.index[-1]
    start_date = end_date - timedelta(days=90)
    filtered_allocation_df = allocation_df[(allocation_df.index >= start_date) & (allocation_df.index <= end_date)]

    # Calculate portions for the area chart
    portion_filtered_df = filtered_allocation_df.div(filtered_allocation_df.sum(axis=1), axis=0).fillna(0)

    # Filter out columns with all zero allocations
    portion_filtered_df = portion_filtered_df.loc[:, (portion_filtered_df != 0).any(axis=0)]

    # Create a stacked area chart for allocation over time
    allocation_area_fig = px.area(
        portion_filtered_df,
        labels={"index": "", "value": "Allocation Proportion"},
        color_discrete_sequence=px.colors.qualitative.Set1
    )
    allocation_area_fig.update_layout(
        title_x=0.5,
        margin=dict(l=40, r=40, t=40, b=80),
        height=600,
        width=800,
        xaxis_title="",
        font=dict(size=16),
        legend=dict(font=dict(size=18), orientation='h', x=0.5, xanchor='center', y=-0.2),
        legend_title_text='',
        plot_bgcolor='white',  # Set plot background to white
        paper_bgcolor='white',  # Set paper background to white
        xaxis=dict(showgrid=True, gridcolor='lightgray'),  # Set x-axis grid lines to gray
        yaxis=dict(showgrid=True, gridcolor='lightgray')   # Set y-axis grid lines to gray
    )

    # Calculate weighted return
    summary_stats_df["balETH_weighted_return"] = (compositeReturn_df * portion_filtered_df).sum(axis=1)
    compositeReturn_df["balETH_weighted_return"] = (compositeReturn_df * portion_filtered_df).sum(axis=1)

    # Create a line chart for weighted return
    weighted_return_fig = px.line(
        summary_stats_df,
        x=summary_stats_df.index,
        y="balETH_weighted_return",
        line_shape="linear",
        markers=True
    )

    weighted_return_fig.update_traces(
        line=dict(width=8),
        line_color="blue",
        line_width=4,
        line_dash = "dash",
        marker=dict(size=10, symbol='circle', color='blue') 
    )

    weighted_return_fig.update_layout(
        title_x=0.5,
        margin=dict(l=40, r=40, t=40, b=80),
        height=600,
        width=800,
        font=dict(size=16),
        yaxis_title="Weighted Return (%)",
        xaxis_title="",
        legend=dict(font=dict(size=18), orientation='h', x=0.5, xanchor='center', y=-0.2),
        legend_title_text='',
        plot_bgcolor='white',
        paper_bgcolor='white',
        xaxis=dict(showgrid=True, gridcolor='lightgray'),
        yaxis=dict(showgrid=True, gridcolor='lightgray')
    )

    # Create a combined line chart for weighted return and composite return
    columns_to_plot = compositeReturn_df.columns[:]

    combined_return_fig = px.line(
        compositeReturn_df,
        x=compositeReturn_df.index,
        y=columns_to_plot,
        markers=False
    )
    combined_return_fig.update_traces(
        line=dict(width=8),
        selector=dict(name="balETH_weighted_return"),
        line_color="blue",
        line_dash="dash",
        line_width=4,
        marker=dict(size=10, symbol='circle', color='blue') 
    )
    combined_return_fig.update_layout(
        title_x=0.5,
        margin=dict(l=40, r=40, t=40, b=80),
        height=600,
        width=800,
        font=dict(size=16),
        yaxis_title="Return (%)",
        xaxis_title="",
        legend=dict(font=dict(size=18), orientation='h', x=0.5, xanchor='auto', y=-0.2),
        legend_title_text='',
        plot_bgcolor='white',
        paper_bgcolor='white',
        xaxis=dict(showgrid=True, gridcolor='lightgray'),
        yaxis=dict(showgrid=True, gridcolor='lightgray')
    )

    # Prepare data for pie chart
    pie_df = allocation_df.copy()
    pie_df["date"] = allocation_df.index
    pie_data = pie_df.groupby("date").max().tail(1).T.reset_index()
    pie_data.columns = ["Destination", "ETH Value"]
    pie_data = pie_data[pie_data["ETH Value"] > 0]

    # Create the pie chart
    lp_allocation_pie_fig = px.pie(
        pie_data,
        names="Destination",
        values="ETH Value",
        color_discrete_sequence=px.colors.qualitative.Pastel
    )
    lp_allocation_pie_fig.update_layout(
        title_x=0.5,
        margin=dict(l=40, r=40, t=40, b=40),
        height=400,
        width=800,
        font=dict(size=16),
        legend=dict(font=dict(size=18), orientation="h", x=0.5, xanchor="center"),
        legend_title_text='',
        plot_bgcolor='white',
        paper_bgcolor='white'
    )
    lp_allocation_pie_fig.update_traces(textinfo='percent+label', hoverinfo='label+value+percent')

    return allocation_area_fig, weighted_return_fig, combined_return_fig, lp_allocation_pie_fig


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



# fetch_data.py
from db_manager import get_last_update, update_last_update, store_data, get_data
from datetime import datetime, timedelta

def fetch_daily_nav():
    last_update = get_last_update('nav_data')
    current_time = datetime.now()
    
    if last_update is None or current_time - last_update > timedelta(days=1):
        # Fetch new data
        nav_df = fetch_new_nav_data()  # Your existing function to fetch nav data
        store_data(nav_df, 'nav_data')
        update_last_update('nav_data')
    else:
        # Load data from database
        nav_df = get_data('nav_data')
    
    return nav_df

def fetch_asset_composition():
    last_update = get_last_update('asset_composition_data')
    current_time = datetime.now()
    
    if last_update is None or current_time - last_update > timedelta(days=1):
        # Fetch new data
        asset_df = fetch_new_asset_composition_data()  # Your existing function to fetch asset composition data
        store_data(asset_df, 'asset_composition_data')
        update_last_update('asset_composition_data')
    else:
        # Load data from database
        asset_df = get_data('asset_composition_data')
    
    return asset_df

# Add more functions for other data types as needed