import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import streamlit as st


from mainnet_launch.constants import BAL_ETH_AUTOPOOL, AUTO_ETH_AUTOPOOL, AUTO_LRT_AUTOPOOL
from mainnet_launch.fetch_key_metrics_data import fetch_key_metric_data



def _apply_default_style(fig:go.Figure) -> None:
    fig.update_traces(line=dict(width=3))
    fig.update_layout(
        title_x=0.5,
        margin=dict(l=40, r=40, t=40, b=80),
        height=400,
        width=800,
        font=dict(size=16),
        xaxis_title="",
        plot_bgcolor="white",
        paper_bgcolor="white",
        xaxis=dict(showgrid=True, gridcolor="lightgray"),
        yaxis=dict(showgrid=True, gridcolor="lightgray"),
    )
    


def display_key_metrics(autoPool:str) -> dict[str, pd.DataFrame]:
    key_metric_data = fetch_key_metric_data(autoPool)
    _show_key_metrics(key_metric_data)
    

    
def _show_key_metrics(key_metric_data:dict[str, pd.DataFrame]):
    st.header("Key Metrics")
    nav_per_share_df = key_metric_data["nav_per_share_df"]
    uwcr_df = key_metric_data["uwcr_df"]
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






def diffReturn(x: list):
    if len(x) < 2:
        return None  # Not enough elements to calculate difference
    return x[-1] - x[-2]