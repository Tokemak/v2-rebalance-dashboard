import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import streamlit as st


from mainnet_launch.constants import AutopoolConstants
from mainnet_launch.pages.autopool_diagnostics.fetch_destination_summary_stats import (
    fetch_destination_summary_stats,
)

from mainnet_launch.pages.solver_diagnostics.solver_rebalance_plans_to_summary_stats import DESTINATION_BLOCK_TABLE
from mainnet_launch.database.database_operations import run_read_only_query


def fetch_and_render_destination_apr_data(autopool: AutopoolConstants) -> go.Figure:
    pool_backing = fetch_destination_summary_stats(autopool, "pool_backing")
    pool_safe_price = fetch_destination_summary_stats(autopool, "pool_safe_price")
    price_return_df = (pool_backing - pool_safe_price).div(pool_backing)
    incentiveApr_df = 100 * fetch_destination_summary_stats(autopool, "incentive_apr") * 0.9
    total_apr_in = 100 * fetch_destination_summary_stats(autopool, "total_apr_in")
    fee_plus_base_apr_df = total_apr_in - incentiveApr_df
    pointsApr_df = 100 * fetch_destination_summary_stats(autopool, "points_apr")

    total_apr_out = 100 * fetch_destination_summary_stats(autopool, "total_apr_out")

    st.subheader("Destination APR Components")

    destination_choice = st.selectbox("Select a destination", pointsApr_df.columns)

    plot_data = pd.DataFrame(
        {
            "Price Return": price_return_df[destination_choice],
            "Base + Fee APR": fee_plus_base_apr_df[destination_choice],
            "Incentive APR": incentiveApr_df[destination_choice],
            "Points APR": pointsApr_df[destination_choice],
            "Total APR In": total_apr_in[destination_choice],
            "Total APR Out": total_apr_out[destination_choice],
        },
        index=pointsApr_df.index,
    )

    st.plotly_chart(px.line(plot_data, title=f"APR Components {destination_choice}"), use_container_width=True)

    st.subheader("APR signals")
    apr_choices = ["Price Return", "Base + Fee APR", "Incentive APR", "Points APR", "Total APR In", "Total APR Out"]
    apr_choice = st.selectbox("Select a Signal", apr_choices)

    if apr_choice == "Price Return":
        fig = px.line(price_return_df, title=apr_choice)
    elif apr_choice == "Base + Fee APR":
        fig = px.line(fee_plus_base_apr_df, title=apr_choice)
    elif apr_choice == "Incentive APR":
        fig = px.line(incentiveApr_df, title=apr_choice)
    elif apr_choice == "Points APR":
        fig = px.line(pointsApr_df, title=apr_choice)
    elif apr_choice == "Total APR In":
        fig = px.line(total_apr_in, title=apr_choice)
    elif apr_choice == "Total APR Out":
        fig = px.line(total_apr_out, title=apr_choice)

    st.plotly_chart(fig, use_container_width=True)

    vault_address_df = run_read_only_query(
        f"""
        SELECT DISTINCT

        destination_vault,
        vault_name, 
        pool_type, 
        pool, 
        underlying
        
        FROM {DESTINATION_BLOCK_TABLE}

        WHERE 

        autopool = ?

        """,
        (autopool.name,),
    )

    with st.expander("Description"):
        st.markdown(
            """                    
                    Price Return (pool backing - pool safe price) / pool backing
                    Fee + Base Apr = Total APR In - Incentive APR
                    """
        )

        st.table(vault_address_df[vault_address_df["destination_vault"] == destination_choice])


if __name__ == "__main__":
    from mainnet_launch.constants import AUTO_USD

    fetch_and_render_destination_apr_data(AUTO_USD)
