import pandas as pd
import plotly.express as px
import streamlit as st
from web3 import Web3

from mainnet_launch.constants import ChainData, ALL_CHAINS, ETH_CHAIN

from mainnet_launch.database.schema.full import Destinations
from mainnet_launch.database.schema.postgres_operations import get_full_table_as_df

from mainnet_launch.database.schema.ensure_tables_are_current.using_onchain.exit_liquidity.update_total_usd_exit_liqudity import (
    get_portion_ownership_by_pool,
)


def _fetch_readable_our_tvl_by_destination(chain: ChainData, block: int) -> pd.DataFrame:
    portion_ownership_df = (
        get_portion_ownership_by_pool(block, chain)
        .rename(columns={"index": "destination_vault_address", "getPool": "pool_address"})[
            ["destination_vault_address", "pool_address", "percent_ownership"]
        ]
        .copy()
    )
    portion_ownership_df["destination_vault_address"] = portion_ownership_df["destination_vault_address"].apply(
        lambda x: Web3.toChecksumAddress(x)
    )

    full_destinations_df = get_full_table_as_df(Destinations, where_clause=Destinations.chain_id == chain.chain_id)[
        [
            "destination_vault_address",
            "underlying_name",
            "underlying_symbol",
            "pool_type",
            "exchange_name",
        ]
    ]

    # add our USD size of position here
    full_destinations_df["destination_vault_address"] = full_destinations_df["destination_vault_address"].apply(
        lambda x: Web3.toChecksumAddress(x)
    )
    df = (
        pd.merge(
            portion_ownership_df,
            full_destinations_df,
            left_on="destination_vault_address",
            right_on="destination_vault_address",
            how="left",
        )
        .sort_values("percent_ownership", ascending=False)
        .reset_index(drop=True)
    )

    return df[
        [
            "underlying_name",
            "percent_ownership",
            "pool_type",
            "exchange_name",
            "destination_vault_address",
            "pool_address",
        ]
    ]


def fetch_and_render_our_percent_ownership_of_each_destination():
    st.subheader("Our Percent Ownership of Each Destination")

    chain: ChainData = st.selectbox(
        "Select Chain", ALL_CHAINS, index=ALL_CHAINS.index(ETH_CHAIN), format_func=lambda x: x.name
    )

    if st.button("Fetch current % ownership by destination"):
        st.subheader("Percent Ownership by Destination")
        our_tvl_by_destination_df = _fetch_readable_our_tvl_by_destination(chain, chain.client.eth.block_number)
        st.dataframe(our_tvl_by_destination_df, use_container_width=True)

    _render_methodology()


def _render_methodology():
    with st.expander("Methodology", expanded=True):
        st.markdown(
            """
            Percent Ownership is calculated by:

            `100 * destination_vault_address.totalSupply() / destination_vault_address.underlyingTotalSupply()`

            At the latest block on the chain.

            It looks out our total ownership regardless of autopool.
            """
        )


if __name__ == "__main__":
    st.set_page_config(page_title="Risk Metrics", layout="wide")
    st.title("Risk Metrics Dashboard")
    fetch_and_render_our_percent_ownership_of_each_destination()
