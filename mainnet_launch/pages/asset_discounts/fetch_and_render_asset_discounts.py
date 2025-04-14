import pandas as pd
import plotly.express as px
import streamlit as st

from mainnet_launch.constants import ETH_CHAIN, ChainData
from mainnet_launch.database.database_operations import (
    ensure_table_has_current_data_by_chain,
    get_all_rows_in_table_by_chain,
)
from mainnet_launch.data_fetching.get_state_by_block import build_blocks_to_use, get_raw_state_by_blocks
from mainnet_launch.pages.asset_discounts.fetch_eth_asset_discounts import build_lst_safe_price_and_backing_calls
from mainnet_launch.pages.asset_discounts.fetch_usd_asset_discounts import (
    build_stablecoin_safe_price_and_backing_calls,
    stablecoin_tuples,
)


# not certain if this should point at the rebalance
ASSET_SAFE_PRICE_AND_BACKING = "ASSET_SAFE_PRICE_AND_BACKING"


def fetch_asset_price_discount_from_external_source(start_block: int, chain: ChainData) -> pd.DataFrame:
    if chain != ETH_CHAIN:
        raise ValueError(f"Only checking prices on mainnet, only expected ETH_CHAIN found {chain.name=}")

    lst_calls = build_lst_safe_price_and_backing_calls()
    stablecoin_calls = build_stablecoin_safe_price_and_backing_calls()
    blocks = build_blocks_to_use(ETH_CHAIN, start_block)

    wide_df = get_raw_state_by_blocks([*lst_calls, *stablecoin_calls], blocks, ETH_CHAIN)
    # long_df has columns timestamp, symbol, backing, safe_price
    # this format is so that the table won't break when a new token is added
    long_df = _convert_wide_df_to_long(wide_df)
    return long_df


def _convert_wide_df_to_long(wide_df: pd.DataFrame) -> pd.DataFrame:
    # might need to remove block
    backing_cols = [col for col in wide_df.columns if col.endswith("_backing")]
    backing_df = wide_df[backing_cols].copy()
    backing_df.columns = [col.replace("_backing", "") for col in backing_df.columns]
    long_backing_df = backing_df.reset_index().melt(id_vars=["timestamp"], var_name="symbol", value_name="backing")

    safe_price_cols = [col for col in wide_df.columns if col.endswith("_safe_price")]
    safe_price_df = wide_df[safe_price_cols].copy()
    safe_price_df.columns = [col.replace("_safe_price", "") for col in safe_price_df.columns]
    long_safe_price_df = safe_price_df.reset_index().melt(
        id_vars=["timestamp"], var_name="symbol", value_name="safe_price"
    )

    long_df = pd.merge(long_backing_df, long_safe_price_df, on=["timestamp", "symbol"])
    return long_df


def _long_df_to_backing_safe_price_and_discount_dfs(long_df: pd.DataFrame):
    backing_df = long_df.reset_index()[["timestamp", "symbol", "backing"]].pivot(
        index="timestamp", columns="symbol", values="backing"
    )
    safe_price_df = long_df.reset_index()[["timestamp", "symbol", "safe_price"]].pivot(
        index="timestamp", columns="symbol", values="safe_price"
    )
    percent_discount_df = long_df.reset_index()[["timestamp", "symbol", "percent_discount"]].pivot(
        index="timestamp", columns="symbol", values="percent_discount"
    )

    return percent_discount_df, safe_price_df, backing_df


def make_sure_safe_price_and_backing_rows_are_in_table():
    # note, I don't like this pattern of a seperate function here all the time
    ensure_table_has_current_data_by_chain(
        table_name=ASSET_SAFE_PRICE_AND_BACKING,
        fetch_data_from_external_source_function=fetch_asset_price_discount_from_external_source,
        only_mainnet=True,
    )


def fetch_and_render_asset_oracle_and_backing():
    make_sure_safe_price_and_backing_rows_are_in_table()

    long_df = get_all_rows_in_table_by_chain(ASSET_SAFE_PRICE_AND_BACKING, ETH_CHAIN)
    long_df["percent_discount"] = 100 * ((long_df["safe_price"] - long_df["backing"]) / long_df["backing"])

    stablecoin_symbols = [a[1] for a in stablecoin_tuples]

    long_stablecoin_df = long_df[long_df["symbol"].isin(stablecoin_symbols)].copy()

    # this just shows the time after autoUSD launched
    # not attached to this, but we don't have safe prices from before this point
    # so there is not a clean way to make the charts line up
    long_stablecoin_df.loc[long_stablecoin_df.index < "Mar-12-2025", ["safe_price", "backing", "percent_discount"]] = (
        None
    )

    long_lst_df = long_df[~long_df["symbol"].isin(stablecoin_symbols)].copy()

    st.title("Asset Safe Price and Backing")
    for (
        name,
        sub_long_df,
    ) in zip(
        ["LSTs and LRTs", "Stablecoins"],
        [
            long_lst_df,
            long_stablecoin_df,
        ],
    ):
        st.header(name)
        percent_discount_df, safe_price_df, backing_df = _long_df_to_backing_safe_price_and_discount_dfs(sub_long_df)

        st.plotly_chart(
            px.line(percent_discount_df, title="Percent Discount").update_yaxes(title_text="percent"),
            use_container_width=True,
        )

        if name == "Stablecoins":
            st.plotly_chart(
                px.line(safe_price_df, title="Safe Price").update_yaxes(title_text="USDC"), use_container_width=True
            )
            st.plotly_chart(
                px.line(backing_df, title="Backing").update_yaxes(title_text="USD"), use_container_width=True
            )

        elif name == "LSTs and LRTs":
            st.plotly_chart(
                px.line(safe_price_df, title="Safe Price").update_yaxes(title_text="ETH"), use_container_width=True
            )
            st.plotly_chart(
                px.line(backing_df, title="Backing").update_yaxes(title_text="ETH"), use_container_width=True
            )

    with st.expander("Description"):
        st.markdown(
            """
- **Percent Discount**

100 * (Safe Price - Backing) / Backing)

- **Safe Price for LSTs and LRTs**

`RootPriceOracle.getPriceinETH()` [0x61F8BE7FD721e80C0249829eaE6f0DAf21bc2CaC](https://etherscan.io/address/0x61F8BE7FD721e80C0249829eaE6f0DAf21bc2CaC)


- **Safe Price for Stablecoins**

`SolverRootOracle.getPriceInQuote(token,USDC)` [0xdb8747a396d75d576dc7a10bb6c8f02f4a3c20f1](https://etherscan.io/address/0xdb8747a396d75d576dc7a10bb6c8f02f4a3c20f1)

- **Backing for LSTs and LRTs**

`LST_calculator.calculateEthPerToken()`

- **Backing for Stablecoins**

Backing is the custom backing call for each token or 1.0, if there is no onchain source and the implied price is 1.0

eg for sUSDe it is  
`sUSDs.convertToAssets(uint256)(uint256)`

and 

USDT = 1.0

Note: new stablecoins must be added manually to these charts
            """
        )


if __name__ == "__main__":
    fetch_and_render_asset_oracle_and_backing()
