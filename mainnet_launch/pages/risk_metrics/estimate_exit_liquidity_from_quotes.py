import asyncio
import streamlit as st
import plotly.express as px
import pandas as pd
from concurrent.futures import ThreadPoolExecutor


from mainnet_launch.constants import *
from mainnet_launch.data_fetching.quotes.get_all_underlying_reserves import fetch_raw_amounts_by_destination

from mainnet_launch.pages.risk_metrics.drop_down import render_pick_chain_and_base_asset_dropdown
from mainnet_launch.pages.risk_metrics.percent_ownership_by_destination import (
    fetch_readable_our_tvl_by_destination,
)

from mainnet_launch.database.schema.full import Destinations, AutopoolDestinations, Tokens, SwapQuote
from mainnet_launch.database.schema.postgres_operations import (
    get_full_table_as_df,
    insert_avoid_conflicts,
    get_highest_value_in_field_where,
)

from mainnet_launch.data_fetching.internal.fetch_quotes import (
    fetch_many_swap_quotes_from_internal_api,
    TokemakQuoteRequest,
)
from mainnet_launch.data_fetching.odos.fetch_quotes import fetch_many_odos_raw_quotes, OdosQuoteRequest

ATTEMPTS = 3
STABLE_COINS_REFERENCE_QUANTITY = 10_000
ETH_REFERENCE_QUANTITY = 5
PERCENT_OWNERSHIP_THRESHOLD = 10  # what percent of a pool can we do we own before we exclude it from odos quotes

USD_SCALED_SIZES = [10_000, 20_000, 50_000, 100_000, 200_000]
ETH_SCALED_SIZES = [5, 20, 50, 100, 200]

PORTIONS = [0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 1.0]

ALLOWED_SIZE_FACTORS = ["portion", "absolute"]
TIME_TO_SLEEP_BETWEEN_QUOTE_ROUNDS = 60 * 2  # 2 minutes

# USD_SCALED_SIZES = [10_000, 200_000]
# ETH_SCALED_SIZES = [
#     5,
#     500,
# ]

# PORTIONS = [0.01, 0.1, 1.0]


def _fetch_current_asset_exposure(
    chain: ChainData, valid_autopools: list[AutopoolConstants], block: int
) -> dict[str, int]:
    """Fetches the exposure and pools to exclude for the given chain and base asset."""
    reserve_df = fetch_raw_amounts_by_destination(block, chain)
    valid_autopool_symbols = [autopool.symbol for autopool in valid_autopools]

    reserve_df = reserve_df[reserve_df["autopool_symbol"].isin(valid_autopool_symbols)].copy()
    reserve_df["reserve_amount"] = reserve_df["reserve_amount"].map(int)
    unscaled_asset_exposure = reserve_df.groupby("token_address")["reserve_amount"].sum().to_dict()
    return unscaled_asset_exposure


def fetch_needed_context(chain: ChainData, valid_autopools: list[AutopoolConstants]):

    block = chain.client.eth.block_number
    # TODO I suspect this duplicates work
    unscaled_asset_exposure = _fetch_current_asset_exposure(chain, valid_autopools, block)
    percent_ownership_by_destination_df = fetch_readable_our_tvl_by_destination(chain, block)

    autopool_destinations = get_full_table_as_df(
        AutopoolDestinations,
        where_clause=AutopoolDestinations.autopool_vault_address.in_(a.autopool_eth_addr for a in valid_autopools),
    )
    these_autopools_destinations = autopool_destinations["destination_vault_address"].unique().tolist()
    percent_ownership_by_destination_df = percent_ownership_by_destination_df[
        percent_ownership_by_destination_df["destination_vault_address"].isin(these_autopools_destinations)
    ].copy()

    token_df = get_full_table_as_df(
        Tokens,
        where_clause=Tokens.chain_id == chain.chain_id,
    )

    return unscaled_asset_exposure, percent_ownership_by_destination_df, token_df


def _build_quote_requests_from_absolute_sizes(
    chain: ChainData,
    base_asset: TokemakAddress,
    unscaled_asset_exposure: dict[str, int],
    percent_ownership_by_destination_df: pd.DataFrame,
    token_df: pd.DataFrame,
) -> tuple[list[TokemakQuoteRequest], list[OdosQuoteRequest]]:

    token_to_decimal = token_df.set_index("token_address")["decimals"].to_dict()

    tokemak_quote_requests = []
    odos_quote_requests = []

    poolBlacklist = (
        percent_ownership_by_destination_df[
            percent_ownership_by_destination_df["percent_ownership"] > PERCENT_OWNERSHIP_THRESHOLD
        ]["pool_address"]
        .unique()
        .tolist()
    )

    if base_asset(chain) == WETH(chain):
        sizes = ETH_SCALED_SIZES
    elif (base_asset(chain) == USDC(chain)) or (base_asset(chain) == DOLA(chain)):
        sizes = USD_SCALED_SIZES
    else:
        raise ValueError(f"Unexpected base asset: {base_asset.name}")

    for size in sizes:
        for token_address, _ in unscaled_asset_exposure.items():
            if token_address == base_asset(chain):
                # Skip the base asset itself, as we don't need to quote it against itself
                continue

            decimals = token_to_decimal[token_address]
            unscaled_amount_times_size = int(size * 10**decimals)

            tokemak_quote_requests.append(
                TokemakQuoteRequest(
                    chain_id=chain.chain_id,
                    token_in=token_address,
                    token_out=base_asset(chain),
                    unscaled_amount_in=unscaled_amount_times_size,
                )
            )

            odos_quote_requests.append(
                OdosQuoteRequest(
                    chain_id=chain.chain_id,
                    token_in=token_address,
                    token_out=base_asset(chain),
                    unscaled_amount_in=unscaled_amount_times_size,
                    poolBlacklist=poolBlacklist,
                )
            )

    return tokemak_quote_requests, odos_quote_requests


def _build_quote_requests_from_portions(
    chain: ChainData,
    base_asset: TokemakAddress,
    unscaled_asset_exposure: dict[str, int],
    percent_ownership_by_destination_df: pd.DataFrame,
    token_df: pd.DataFrame,
) -> tuple[list[TokemakQuoteRequest], list[OdosQuoteRequest]]:
    """Builds a list of TokemakQuoteRequest objects for the given chain and base asset."""

    tokemak_quote_requests = []
    odos_quote_requests = []

    poolBlacklist = (
        percent_ownership_by_destination_df[
            percent_ownership_by_destination_df["percent_ownership"] > PERCENT_OWNERSHIP_THRESHOLD
        ]["pool_address"]
        .unique()
        .tolist()
    )

    for portion in PORTIONS:
        for token_address, amount in unscaled_asset_exposure.items():
            if token_address == base_asset(chain):
                # Skip the base asset itself, as we don't need to quote it against itself
                continue

            unscaled_amount_times_portion = int(amount * portion)

            tokemak_quote_requests.append(
                TokemakQuoteRequest(
                    chain_id=chain.chain_id,
                    token_in=token_address,
                    token_out=base_asset(chain),
                    unscaled_amount_in=unscaled_amount_times_portion,
                )
            )

            odos_quote_requests.append(
                OdosQuoteRequest(
                    chain_id=chain.chain_id,
                    token_in=token_address,
                    token_out=base_asset(chain),
                    unscaled_amount_in=unscaled_amount_times_portion,
                    poolBlacklist=poolBlacklist,
                )
            )

    return tokemak_quote_requests, odos_quote_requests


def _fetch_several_rounds_of_quotes(tokemak_quote_requests, odos_quote_requests) -> list[SwapQuote]:
    """Fetches several rounds of quotes using the provided fetch function."""
    tokemak_quotes: list[pd.DataFrame] = []
    odos_quotes: list[pd.DataFrame] = []

    with ThreadPoolExecutor(max_workers=2) as executor:
        for i in range(ATTEMPTS):
            future_tokemak = executor.submit(fetch_many_swap_quotes_from_internal_api, tokemak_quote_requests)
            future_odos = executor.submit(fetch_many_odos_raw_quotes, odos_quote_requests)

            some_tokemak_quote_df = future_tokemak.result()
            some_odos_quotes_df = future_odos.result()

            tokemak_quotes.append(some_tokemak_quote_df)
            odos_quotes.append(some_odos_quotes_df)

            if i != ATTEMPTS - 1:
                print(f"sleeping {TIME_TO_SLEEP_BETWEEN_QUOTE_ROUNDS} seconds, in attempts loop")
                time.sleep(TIME_TO_SLEEP_BETWEEN_QUOTE_ROUNDS)

    # Concatenate all results
    raw_tokemak_quote_response_df = pd.concat(tokemak_quotes, ignore_index=True)
    raw_odos_quote_response_df = pd.concat(odos_quotes, ignore_index=True)

    return raw_tokemak_quote_response_df, raw_odos_quote_response_df


def _post_process_raw_tokemak_quote_response_df(
    raw_tokemak_quote_response_df: pd.DataFrame,
    token_df: pd.DataFrame,
    batch_id: int,
    size_factor: str,
    base_asset: str,
) -> list[SwapQuote]:
    token_to_decimal = token_df.set_index("token_address")["decimals"].to_dict()

    quotes: list[SwapQuote] = []
    for _, row in raw_tokemak_quote_response_df.iterrows():
        sell_addr = row["sellToken"]
        buy_addr = row["buyToken"]
        chain_id = int(row["chainId"])

        # scale raw integer amounts by the token decimals
        scaled_in = int(row["sellAmount"]) / 10 ** token_to_decimal[sell_addr]
        scaled_out = int(row["buyAmount"]) / 10 ** token_to_decimal[buy_addr]

        quote = SwapQuote(
            chain_id=chain_id,
            api_name="tokemak",
            sell_token_address=sell_addr,
            buy_token_address=buy_addr,
            scaled_amount_in=scaled_in,
            scaled_amount_out=scaled_out,
            pools_blacklist=None,  # tokemak has no blacklist support
            aggregator_name=row["aggregatorName"],
            datetime_received=row["datetime_received"],
            quote_batch=batch_id,
            size_factor=size_factor,
            base_asset=base_asset,
        )
        quotes.append(quote)

    return quotes


def _post_process_raw_odos_quote_response_df(
    raw_odos_quote_response_df: pd.DataFrame,
    token_df: pd.DataFrame,
    batch_id: int,
    size_factor: str,
    base_asset: str,
) -> list[SwapQuote]:
    # lookup tables for decimals
    token_to_decimal = token_df.set_index("token_address")["decimals"].to_dict()

    quotes: list[SwapQuote] = []
    for _, row in raw_odos_quote_response_df.iterrows():
        # normalize addresses
        sell_addr = Web3.toChecksumAddress(row["inTokens"])
        buy_addr = Web3.toChecksumAddress(row["outTokens"])
        chain_id = int(row["chainId"])

        # raw amounts
        unscaled_in = int(row["inAmounts"])
        unscaled_out = int(row["outAmounts"])

        # apply decimals
        scaled_in = unscaled_in / 10 ** token_to_decimal[sell_addr]
        scaled_out = unscaled_out / 10 ** token_to_decimal[buy_addr]

        quote = SwapQuote(
            chain_id=chain_id,
            api_name="odos",
            sell_token_address=sell_addr,
            buy_token_address=buy_addr,
            scaled_amount_in=scaled_in,
            scaled_amount_out=scaled_out,
            pools_blacklist=str(tuple(row["poolBlacklist"])),
            aggregator_name="Odos",
            datetime_received=row["datetime_received"],
            quote_batch=batch_id,
            size_factor=size_factor,
            base_asset=base_asset,
        )
        quotes.append(quote)

    return quotes


def _add_new_swap_quotes_to_db(
    raw_odos_quote_response_df: pd.DataFrame,
    raw_tokemak_quote_response_df: pd.DataFrame,
    token_df: pd.DataFrame,
    size_factor: str,
    base_asset: str,
    batch_id: int,
) -> None:

    cleaned_odos_responses = _post_process_raw_odos_quote_response_df(
        raw_odos_quote_response_df, token_df, batch_id, size_factor, base_asset
    )
    cleaned_tokemak_responses = _post_process_raw_tokemak_quote_response_df(
        raw_tokemak_quote_response_df, token_df, batch_id, size_factor, base_asset
    )

    time.sleep(2)  # helps with printing readablity
    insert_avoid_conflicts(
        cleaned_odos_responses + cleaned_tokemak_responses,
        SwapQuote,
    )


def fetch_and_save_odos_and_tokemak_quotes(
    chain: ChainData,
    base_asset: TokemakAddress,
    valid_autopools: list[AutopoolConstants],
) -> tuple[pd.DataFrame, pd.DataFrame]:

    unscaled_asset_exposure, percent_ownership_by_destination_df, token_df = fetch_needed_context(
        chain, valid_autopools
    )

    portion_tokemak_quote_requests, portion_odos_quote_requests = _build_quote_requests_from_portions(
        chain, base_asset, unscaled_asset_exposure, percent_ownership_by_destination_df, token_df
    )

    portion_raw_tokemak_quote_response_df, portion_raw_odos_quote_response_df = _fetch_several_rounds_of_quotes(
        tokemak_quote_requests=portion_tokemak_quote_requests,
        odos_quote_requests=portion_odos_quote_requests,
    )

    highest_swap_quote_batch_id = get_highest_value_in_field_where(SwapQuote, SwapQuote.quote_batch, where_clause=None)
    if highest_swap_quote_batch_id is None:
        highest_swap_quote_batch_id = 0
    else:
        highest_swap_quote_batch_id += 1

    _add_new_swap_quotes_to_db(
        raw_odos_quote_response_df=portion_raw_odos_quote_response_df,
        raw_tokemak_quote_response_df=portion_raw_tokemak_quote_response_df,
        token_df=token_df,
        size_factor="portion",
        base_asset=base_asset(chain),
        batch_id=highest_swap_quote_batch_id,
    )

    print("sleeping 2 minutes before absolute quotes")
    time.sleep(TIME_TO_SLEEP_BETWEEN_QUOTE_ROUNDS)

    absolute_tokemak_quote_requests, absolute_odos_quote_requests = _build_quote_requests_from_absolute_sizes(
        chain, base_asset, unscaled_asset_exposure, percent_ownership_by_destination_df, token_df
    )

    absolute_raw_tokemak_quote_response_df, absolute_raw_odos_quote_response_df = _fetch_several_rounds_of_quotes(
        tokemak_quote_requests=absolute_tokemak_quote_requests,
        odos_quote_requests=absolute_odos_quote_requests,
    )

    _add_new_swap_quotes_to_db(
        raw_odos_quote_response_df=absolute_raw_odos_quote_response_df,
        raw_tokemak_quote_response_df=absolute_raw_tokemak_quote_response_df,
        token_df=token_df,
        size_factor="absolute",
        base_asset=base_asset(chain),
        batch_id=highest_swap_quote_batch_id,
    )


@time_decorator
def save_full_round():
    chain_base_asset_groups = {
        (ETH_CHAIN, WETH): (AUTO_ETH, AUTO_LRT, BAL_ETH, DINERO_ETH),
        (ETH_CHAIN, USDC): (AUTO_USD,),
        (ETH_CHAIN, DOLA): (AUTO_DOLA,),
        (SONIC_CHAIN, USDC): (SONIC_USD,),
        (BASE_CHAIN, WETH): (BASE_ETH,),
        (BASE_CHAIN, USDC): (BASE_USD,),
    }

    for k, valid_autopools in chain_base_asset_groups.items():
        chain, base_asset = k

        fetch_and_save_odos_and_tokemak_quotes(chain, base_asset, valid_autopools)
        a = get_highest_value_in_field_where(SwapQuote, SwapQuote.quote_batch, where_clause=None)
        print(a)
        time.sleep(TIME_TO_SLEEP_BETWEEN_QUOTE_ROUNDS)
        print(f"Finished fetching quotes for {chain.name} and {base_asset.name}.")


def fetch_and_render_exit_liquidity_from_quotes():
    st.write('stub')
    pass

if __name__ == "__main__":
    save_full_round()

    # fetch_and_render_exit_liquidity_from_quotes()


# def _post_process_quotes(
#     raw_odos_quote_response_df: pd.DataFrame, raw_tokemak_quote_response_df: pd.DataFrame, token_df: pd.DataFrame
# ) -> pd.DataFrame:

#     cleaned_odos_responses = _post_process_raw_odos_quote_response_df(raw_odos_quote_response_df, token_df)
#     clean_odos_response_df = pd.DataFrame(cleaned_odos_responses)
#     cleaned_tokemak_responses = _post_process_raw_tokemak_quote_response_df(raw_tokemak_quote_response_df, token_df)
#     clean_tokemak_response_df = pd.DataFrame(cleaned_tokemak_responses)

#     df = pd.concat([clean_odos_response_df, clean_tokemak_response_df], ignore_index=True)

#     df["effective_price"] = df["scaled_amount_out"] / df["scaled_amount_in"]
#     df["label"] = df["token_in_symbol"] + " - " + df["api"]
#     return df


# def fetch_and_render_exit_liquidity_from_quotes() -> None:
#     st.subheader("Exit Liquidity Quote Explorer")
#     chain, base_asset, valid_autopools = render_pick_chain_and_base_asset_dropdown()
#     tokemak_quote_requests_df, odos_quote_requests_df, tokemak_response_df, odos_response_df, df = (
#         fetch_odos_and_tokemak_quotes(chain, base_asset, valid_autopools)
#     )

#     st.dataframe(df)
#     pass


# def _fetch_quote_and_slippage_data(valid_autopools: tuple[AutopoolConstants]):
#     a_valid_autopool = valid_autopools[0]

#     block = a_valid_autopool.chain.client.eth.block_number
#     reserve_df = fetch_raw_amounts_by_destination(block, a_valid_autopool.chain)
#     valid_autopool_symbols = [pool.symbol for pool in valid_autopools]
#     reserve_df = reserve_df[reserve_df["autopool_symbol"].isin(valid_autopool_symbols)].copy()
#     reserve_df["reserve_amount"] = reserve_df["reserve_amount"].map(int)

#     balances = reserve_df.groupby("token_address")["reserve_amount"].sum().to_dict()
#     st.write("Balances")
#     st.write(balances, use_container_width=True)

#     quote_df, slippage_df = fetch_quotes(
#         a_valid_autopool.chain, a_valid_autopool.base_asset, a_valid_autopool.base_asset_decimals, balances
#     )

#     return quote_df, slippage_df


# def _render_slippage_plots(slippage_df: pd.DataFrame) -> None:
#     slippage_df_not_reference_price = slippage_df[
#         slippage_df["reference_quantity"] != slippage_df["Sold Quantity"].astype(int)
#     ]

#     pivot_df = (
#         slippage_df_not_reference_price.pivot(
#             index="percent_sold", columns="symbol", values="bps_loss_excess_vs_reference_price"
#         )
#         .sort_index()
#         .dropna(how="any")
#     )

#     st.subheader("Excess Slippage (bps) by Percent Sold")
#     st.dataframe(pivot_df, use_container_width=True)

#     st.plotly_chart(
#         px.scatter(
#             slippage_df,
#             x="percent_sold",
#             y="bps_loss_excess_vs_reference_price",
#             color="symbol",
#             hover_data={"Sold Quantity": ":.2f"},
#             title="Excess slippage bps by % sold",
#         )
#     )


# def fetch_and_render_exit_liquidity_from_quotes() -> None:
#     st.subheader("Exit Liquidity Quote Explorer")
#     chain_base_asset_groups = {
#         (ETH_CHAIN, WETH): (AUTO_ETH, AUTO_LRT, BAL_ETH, DINERO_ETH),
#         (ETH_CHAIN, USDC): (AUTO_USD,),
#         (ETH_CHAIN, DOLA): (AUTO_DOLA,),
#         (SONIC_CHAIN, USDC): (SONIC_USD,),
#         (BASE_CHAIN, WETH): (BASE_ETH,),
#         (BASE_CHAIN, USDC): (BASE_USD,),
#     }

#     options = list(chain_base_asset_groups.keys())
#     chain, base_asset = st.selectbox(
#         "Pick a Chain & Base Asset:", options, format_func=lambda k: f"{k[0].name} chain and {k[1].name}"
#     )
#     autopools = chain_base_asset_groups[(chain, base_asset)]

#     _render_methodology()

#     quote_df, slippage_df = _fetch_quote_and_slippage_data(autopools)
#     _render_slippage_plots(slippage_df)
#     _render_download_raw_quote_data_buttons(quote_df, slippage_df)


# def _render_download_raw_quote_data_buttons(quote_df: pd.DataFrame, slippage_df: pd.DataFrame) -> None:
#     """Adds two Streamlit download buttons for the raw data."""
#     csv_quotes = quote_df.to_csv(index=False).encode("utf-8")
#     csv_slippage = slippage_df.to_csv(index=False).encode("utf-8")

#     st.download_button(
#         label="Download Full Quote Data",
#         data=csv_quotes,
#         file_name="quote_data.csv",
#         mime="text/csv",
#     )
#     st.download_button(
#         label="Download Full Slippage Data",
#         data=csv_slippage,
#         file_name="slippage_data.csv",
#         mime="text/csv",
#     )


# def _render_methodology():
#     with st.expander("See Methodology"):
#         st.markdown(
#             """
# # Estimating Excess Slippage From Price Impact on Asset Exits

# This method helps quantify how much extra slippage we incur when selling larger chunks of our assets.

# 1. Reference Price

# - Execute a small “reference” sale

# - For stablecoins: sell 10 000 units

# - For LSTs/LRTs: sell 5 units

# Compute the reference price
# - Example: sell 5 stETH → receive 4.9 ETH
# - Reference price = 4.9 ETH ÷ 5 stETH = 0.98 ETH/stETH

# 2. Measuring Excess Slippage

# - Sell a larger quantity (e.g., 100 stETH → receive 97.5 ETH)
# - New price = 97.5 ETH ÷ 100 stETH = 0.975 ETH/stETH

# - Excess slippage in basis points (bps):

# `slippage_bps = 10 000 * (0.98 - 0.975) ÷ 0.98 ≈ 51 bps`

# - This tells you how far the large-sale price has fallen relative to our reference.

# 3. Key Details

# - The quote data source is our swapper API at https://swaps-pricing.tokemaklabs.com/swap-quote-v2.

# - Use buyAmount (not minBuyAmount) in all calculations.

# - Percent-based scaling: looks at the current balance across each autopool and sells a percentage of it.

# - Additional stablecoin checks at quantities [50 000, 100 000, 200 000].

# - Deliberately slow: Because of various DEX-aggregator rate limits we need to be slower to avoid spurious 50-90 % “losses” on large sales.

# - Outlier mitigation: for each size, perform three quotes (with 12s then and 24s delays) and report the median.

# - No data is saved: all data is fetched live each run.

# - Because there is latency between the quote requests, the quotes are for different blocks so they are not 1:1 comparable with each other. Treat them as directionally correct rather than exact.

# 4. Known Issues

# - If we are a large share of the pool (e.g. most of pxETH:ETH liquidity), the large-sale quotes can look artificially better because in the real world we would be effectively trading against ourselves.
# """
#         )


# if __name__ == "__main__":
#     st.set_page_config(page_title="Exit Liquidity Explorer", layout="wide")
#     fetch_and_render_exit_liquidity_from_quotes()

# # streamlit run mainnet_launch/pages/exit_liquidity/estimate_exit_liquidity_from_quotes.py


# # this can be added in the email version
# # UI CHANGES
# # user can put in threshold -> answer all the token that are a problem
# # include a doc explain the assumptions)
# # dynamic coloring, as well
# # if slippage > X -> then make the cell yellow
# # maybe also show it as a table as well
# # don't over crowd it
