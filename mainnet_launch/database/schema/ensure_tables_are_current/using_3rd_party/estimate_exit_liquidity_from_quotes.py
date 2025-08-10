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
from mainnet_launch.data_fetching.fetch_data_from_3rd_party_api import THIRD_PARTY_SUCCESS_KEY


# need to think more deeply about a fast way to do this.

# how about we make all the requests first,
# then x3 the requests

# then, send them to the APIs, with the proper rate limiters
# I think that is the way

# don't fail on timeout, just send it back to the queue


# move to update database
# make a seperate update file
# TODO add enums instead of strings

ATTEMPTS = 3
STABLE_COINS_REFERENCE_QUANTITY = 10_000
ETH_REFERENCE_QUANTITY = 5
PERCENT_OWNERSHIP_THRESHOLD = 10  # what percent of a pool can we do we own before we exclude it from odos quotes

USD_SCALED_SIZES = [10_000, 20_000, 50_000, 100_000, 200_000]
ETH_SCALED_SIZES = [5, 20, 50, 100, 200]

PORTIONS = [0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 1.0]

ALLOWED_SIZE_FACTORS = ["portion", "absolute"]
TIME_TO_SLEEP_BETWEEN_QUOTE_ROUNDS = 5 * 60  # 2 minutes got 1440 on 2 min sleep, checking 1 min sleeping, 1 min failed


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
        if THIRD_PARTY_SUCCESS_KEY not in row:
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
                percent_exclude_threshold=PERCENT_OWNERSHIP_THRESHOLD,
            )
            quotes.append(quote)
            # silently skip quotes that failed

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
        if THIRD_PARTY_SUCCESS_KEY not in row:
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
                percent_exclude_threshold=PERCENT_OWNERSHIP_THRESHOLD,
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


def fetch_and_save_all_at_once():
    chain_base_asset_groups = {
        (ETH_CHAIN, WETH): (AUTO_ETH, AUTO_LRT, BAL_ETH, DINERO_ETH),
        (ETH_CHAIN, USDC): (AUTO_USD,),
        (ETH_CHAIN, DOLA): (AUTO_DOLA,),
        (SONIC_CHAIN, USDC): (SONIC_USD,),
        (BASE_CHAIN, WETH): (BASE_ETH,),
        (BASE_CHAIN, USDC): (BASE_USD,),
    }

    highest_swap_quote_batch_id = get_highest_value_in_field_where(SwapQuote, SwapQuote.quote_batch, where_clause=None)
    if highest_swap_quote_batch_id is None:
        highest_swap_quote_batch_id = 0
    else:
        highest_swap_quote_batch_id += 1

    for size_factor, quote_bulding_function in zip(
        ["absolute", "portion"],
        [
            _build_quote_requests_from_absolute_sizes,
            _build_quote_requests_from_portions,
        ],
    ):
        print(f"Using {quote_bulding_function.__name__} to build quote requests.")

        all_tokemak_requests = []
        all_odos_requests = []
        for k, valid_autopools in chain_base_asset_groups.items():
            chain, base_asset = k

            unscaled_asset_exposure, percent_ownership_by_destination_df, token_df = fetch_needed_context(
                chain, valid_autopools
            )

            portion_tokemak_quote_requests, portion_odos_quote_requests = quote_bulding_function(
                chain, base_asset, unscaled_asset_exposure, percent_ownership_by_destination_df, token_df
            )

            all_tokemak_requests.extend(portion_tokemak_quote_requests)
            all_odos_requests.extend(portion_odos_quote_requests)

        # # we want 3 of each request to get the median

        all_tokemak_requests = all_tokemak_requests * 3
        all_odos_requests = all_odos_requests * 3

        print(f'Fetching {len(all_odos_requests)} Odos quotes and {len(all_tokemak_requests)} Tokemak quotes.')

        odos_quote_response_df = fetch_many_odos_raw_quotes(all_odos_requests)
        tokemak_quote_response_df = fetch_many_swap_quotes_from_internal_api(all_tokemak_requests)

        for k, valid_autopools in chain_base_asset_groups.items():
            chain, base_asset = k

            sub_odos_df = odos_quote_response_df[
                (odos_quote_response_df["chainId"] == chain.chain_id)
                & (odos_quote_response_df["outTokens"].apply(lambda x: x[0]) == base_asset(chain))
            ].reset_index(drop=True)
            sub_tokemak_df = tokemak_quote_response_df[
                (tokemak_quote_response_df["chainId"] == chain.chain_id)
                & (tokemak_quote_response_df["buyToken"] == base_asset(chain))
            ].reset_index(drop=True)

            _add_new_swap_quotes_to_db(
                raw_odos_quote_response_df=sub_odos_df,
                raw_tokemak_quote_response_df=sub_tokemak_df,
                token_df=token_df,
                size_factor=size_factor,
                base_asset=base_asset(chain),
                batch_id=highest_swap_quote_batch_id,
            )
        else:
            print(f"No quotes found for {chain.name} and {base_asset.name} in odos and tokemak.")


# def fetch_and_save_odos_and_tokemak_quotes(
#     chain: ChainData,
#     base_asset: TokemakAddress,
#     valid_autopools: list[AutopoolConstants],
# ) -> None:

#     unscaled_asset_exposure, percent_ownership_by_destination_df, token_df = fetch_needed_context(
#         chain, valid_autopools
#     )

#     portion_tokemak_quote_requests, portion_odos_quote_requests = _build_quote_requests_from_portions(
#         chain, base_asset, unscaled_asset_exposure, percent_ownership_by_destination_df, token_df
#     )
#     if (len(portion_tokemak_quote_requests) == 0) and len(portion_odos_quote_requests) == 0:
#         # eg if we only hold the base asset on that chain, we don't need to check liquidity
#         # early exit
#         return

#     portion_raw_tokemak_quote_response_df, portion_raw_odos_quote_response_df = _fetch_several_rounds_of_quotes(
#         tokemak_quote_requests=portion_tokemak_quote_requests,
#         odos_quote_requests=portion_odos_quote_requests,
#     )

#     highest_swap_quote_batch_id = get_highest_value_in_field_where(SwapQuote, SwapQuote.quote_batch, where_clause=None)
#     if highest_swap_quote_batch_id is None:
#         highest_swap_quote_batch_id = 0
#     else:
#         highest_swap_quote_batch_id += 1

#     _add_new_swap_quotes_to_db(
#         raw_odos_quote_response_df=portion_raw_odos_quote_response_df,
#         raw_tokemak_quote_response_df=portion_raw_tokemak_quote_response_df,
#         token_df=token_df,
#         size_factor="portion",
#         base_asset=base_asset(chain),
#         batch_id=highest_swap_quote_batch_id,
#     )

#     print("sleeping 2 minutes before absolute quotes")
#     time.sleep(TIME_TO_SLEEP_BETWEEN_QUOTE_ROUNDS)

#     absolute_tokemak_quote_requests, absolute_odos_quote_requests = _build_quote_requests_from_absolute_sizes(
#         chain, base_asset, unscaled_asset_exposure, percent_ownership_by_destination_df, token_df
#     )

#     absolute_raw_tokemak_quote_response_df, absolute_raw_odos_quote_response_df = _fetch_several_rounds_of_quotes(
#         tokemak_quote_requests=absolute_tokemak_quote_requests,
#         odos_quote_requests=absolute_odos_quote_requests,
#     )

#     _add_new_swap_quotes_to_db(
#         raw_odos_quote_response_df=absolute_raw_odos_quote_response_df,
#         raw_tokemak_quote_response_df=absolute_raw_tokemak_quote_response_df,
#         token_df=token_df,
#         size_factor="absolute",
#         base_asset=base_asset(chain),
#         batch_id=highest_swap_quote_batch_id,
#     )


# def save_full_round():
#     chain_base_asset_groups = {
#         (ETH_CHAIN, WETH): (AUTO_ETH, AUTO_LRT, BAL_ETH, DINERO_ETH),
#         (ETH_CHAIN, USDC): (AUTO_USD,),
#         (ETH_CHAIN, DOLA): (AUTO_DOLA,),
#         (SONIC_CHAIN, USDC): (SONIC_USD,),
#         (BASE_CHAIN, WETH): (BASE_ETH,),
#         (BASE_CHAIN, USDC): (BASE_USD,),
#     }

#     for k, valid_autopools in chain_base_asset_groups.items():
#         chain, base_asset = k
#         # print(f"Fetching quotes for {chain.name} and {base_asset.name}.")
#         fetch_and_save_odos_and_tokemak_quotes(chain, base_asset, valid_autopools)
#         # time.sleep(TIME_TO_SLEEP_BETWEEN_QUOTE_ROUNDS)
#         # print(f"Finished fetching quotes for {chain.name} and {base_asset.name}.")


if __name__ == "__main__":
    fetch_and_save_all_at_once()


# options for faster,
# group together all the requsts into one
# use less appempts
# use less sizes, portions etc
