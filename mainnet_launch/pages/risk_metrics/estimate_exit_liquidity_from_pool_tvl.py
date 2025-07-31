"""Note missing fluid"""

import streamlit as st
import pandas as pd

from mainnet_launch.constants import *

from mainnet_launch.database.schema.ensure_tables_are_current.using_onchain.exit_liquidity.update_total_usd_exit_liqudity import (
    fetch_exit_liqudity_tvl,
)


st.cache_data(ttl=5 * 60)


def _fetch_data(chain: ChainData, base_asset: TokemakAddress, refresh: bool):
    (
        valid_dex_df,
        all_chain_asset_exposure_df,
        our_token_to_total_other_token_liquidity,
        token_symbol_to_dfs,
        portion_ownership_by_destination_df,
        coingecko_prices,
    ) = fetch_exit_liqudity_tvl(chain, base_asset, refresh)

    wide_exit_liquidity_df, total_usd_exit_liqudity_df, this_combination_exposure_df = _compute_readable_exit_liquidity(
        all_chain_asset_exposure_df,
        valid_dex_df,
        chain,
        base_asset,
    )

    exit_liquidity_and_exposure_df = combine_our_exposure_with_exit_liquidity(
        this_combination_exposure_df,
        total_usd_exit_liqudity_df,
        coingecko_prices,
    )

    return exit_liquidity_and_exposure_df, wide_exit_liquidity_df, valid_dex_df


def _compute_readable_exit_liquidity(
    all_chain_asset_exposure_df: pd.DataFrame,
    valid_dex_df: pd.DataFrame,
    chain: ChainData,
    base_asset: TokemakAddress,
):
    tokens_to_check_exit_liqudity_for = (
        all_chain_asset_exposure_df[
            (all_chain_asset_exposure_df["chain_id"] == chain.chain_id)
            & (all_chain_asset_exposure_df["reference_asset"] == base_asset(chain))
        ]["token_address"]
        .unique()
        .tolist()
    )

    our_token_to_total_other_token_liquidity = {}
    token_symbol_to_dfs = {}

    for token in tokens_to_check_exit_liqudity_for:
        # also works by token address
        sub_df = valid_dex_df[
            (valid_dex_df["base_token_address"] == token) | (valid_dex_df["quote_token_address"] == token)
        ]
        if sub_df.empty:
            continue

        quote_token_is_target = sub_df[(sub_df["quote_token_address"] == token)]
        quote_liqudity = quote_token_is_target.groupby("base_token_symbol")["scaled_base_usd_liquidity"].sum().to_dict()

        base_token_is_target = sub_df[(sub_df["base_token_address"] == token)]
        base_liqudity = base_token_is_target.groupby("quote_token_symbol")["scaled_quote_usd_liquidity"].sum().to_dict()

        for k, v in base_liqudity.items():
            if k not in quote_liqudity:
                quote_liqudity[k] = 0
            quote_liqudity[k] += v

        our_token_symbol = (
            quote_token_is_target["quote_token_symbol"].iloc[0]
            if not quote_token_is_target.empty
            else base_token_is_target["base_token_symbol"].iloc[0]
        )

        our_token_to_total_other_token_liquidity[our_token_symbol] = quote_liqudity
        token_symbol_to_dfs[our_token_symbol] = sub_df

    our_token_to_total_other_token_liquidity

    rows = []
    for from_token, to_token_and_usd_liquidity in our_token_to_total_other_token_liquidity.items():
        for to_token, usd_liqudity in to_token_and_usd_liquidity.items():
            if usd_liqudity > 50_000:
                rows.append({"from_token": from_token, "to_token": to_token, "total_exit_liqudity": usd_liqudity})

    this_combination_exposure_df = all_chain_asset_exposure_df[
        (all_chain_asset_exposure_df["chain_id"] == chain.chain_id)
        & (all_chain_asset_exposure_df["reference_asset"] == base_asset(chain))
    ].copy()

    exit_liqudity_df = pd.DataFrame(rows)

    wide_exit_liquidity_df = (
        exit_liqudity_df.pivot(index="from_token", columns="to_token", values="total_exit_liqudity").fillna(0).T.round()
    )
    total_usd_exit_liqudity_df = (
        exit_liqudity_df.groupby("from_token")["total_exit_liqudity"].sum().round().reset_index()
    )
    return wide_exit_liquidity_df, total_usd_exit_liqudity_df, this_combination_exposure_df


def combine_our_exposure_with_exit_liquidity(
    this_combination_exposure_df: pd.DataFrame,
    total_usd_exit_liqudity_df: pd.DataFrame,
    coingecko_prices: dict,
):

    this_combination_exposure_df["usd_price"] = this_combination_exposure_df["token_address"].map(coingecko_prices)
    this_combination_exposure_df["usd_exposure"] = (
        this_combination_exposure_df["usd_price"] * this_combination_exposure_df["quantity"]
    )
    our_usd_exposure_df = this_combination_exposure_df[
        ["token_symbol", "token_address", "usd_exposure", "quantity"]
    ].round(2)

    exit_liquidity_and_exposure_df = pd.merge(
        our_usd_exposure_df, total_usd_exit_liqudity_df, right_on="from_token", left_on="token_symbol", how="left"
    )

    exit_liquidity_and_exposure_df["our_percent_of_exit_liquidity"] = (
        100 * exit_liquidity_and_exposure_df["usd_exposure"] / exit_liquidity_and_exposure_df["total_exit_liqudity"]
    ).round()
    exit_liquidity_and_exposure_df.drop(columns=["from_token"], inplace=True)
    for col in ["total_exit_liqudity", "usd_exposure"]:
        exit_liquidity_and_exposure_df[col] = exit_liquidity_and_exposure_df[col].apply(lambda x: f"${x:,.0f}")
    exit_liquidity_and_exposure_df = exit_liquidity_and_exposure_df.sort_values("our_percent_of_exit_liquidity")
    exit_liquidity_and_exposure_df = exit_liquidity_and_exposure_df[
        [
            "token_symbol",
            "token_address",
            "usd_exposure",
            "total_exit_liqudity",
            "our_percent_of_exit_liquidity",
            "quantity",
        ]
    ]

    return exit_liquidity_and_exposure_df


def fetch_and_render_exit_liqudity_pools() -> None:
    chain_base_asset_groups = {
        (ETH_CHAIN, WETH): (AUTO_ETH, AUTO_LRT, BAL_ETH, DINERO_ETH),
        (ETH_CHAIN, USDC): (AUTO_USD,),
        (ETH_CHAIN, DOLA): (AUTO_DOLA,),
        (SONIC_CHAIN, USDC): (SONIC_USD,),
        (BASE_CHAIN, WETH): (BASE_ETH,),
        (BASE_CHAIN, USDC): (BASE_USD,),
    }

    options = list(chain_base_asset_groups.keys())
    chain, base_asset = st.selectbox(
        "Pick a Chain & Base Asset:", options, format_func=lambda k: f"{k[0].name} chain and {k[1].name}"
    )
    refresh = st.checkbox("Refresh Data", key="Use Latest Block")
    if st.button("Fetch Exit Liquidity Pools"):
        with st.spinner("Fetching exit liquidity pools..."):

            exit_liquidity_and_exposure_df, wide_exit_liquidity_df, valid_dex_df = _fetch_data(
                chain, base_asset, refresh=refresh
            )

        st.markdown("Our Exposure and Found Exit Liquidity by Token")
        st.dataframe(exit_liquidity_and_exposure_df.round(2))

        st.markdown("What Other Token is the exit liquidity in?")
        st.dataframe(wide_exit_liquidity_df.round(2))

        st.markdown("All found exit liquidity pools")
        st.dataframe(valid_dex_df)


if __name__ == "__main__":
    st.title("Exit Liquidity Pools")
    fetch_and_render_exit_liqudity_pools()
    # render_exit_liquidity_pools(token_symbol_to_dfs)
