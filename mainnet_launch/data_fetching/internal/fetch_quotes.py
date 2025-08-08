import pandas as pd
import streamlit as st
import concurrent.futures

from mainnet_launch.constants import *
from mainnet_launch.database.schema.full import Tokens
from mainnet_launch.database.schema.postgres_operations import get_full_table_as_df
from mainnet_launch.data_fetching.fetch_data_from_3rd_party_api import (
    make_many_requests_to_3rd_party,
    make_single_request_to_3rd_party,
    THIRD_PARTY_SUCCESS_KEY,
)

ATTEMPTS = 3
STABLE_COINS_REFERENCE_QUANTITY = 10_000
ETH_REFERENCE_QUANTITY = 5
PORTIONS_TO_CHECK = [0.01, 0.05, 0.1, 0.25]


@dataclass
class TokemakQuoteRequest:
    chain_id: int
    token_in: str
    token_out: str
    unscaled_amount_in: str


def _process_quote_response(response: dict) -> dict:
    if response[THIRD_PARTY_SUCCESS_KEY]:
        fields_to_keep = ["buyAmount", "aggregatorName", "datetime_received"]
        cleaned_data = {a: response[a] for a in fields_to_keep}
        request_kwargs = response["request_kwargs"]
        json_payload = request_kwargs.pop("json")
        cleaned_data.update(json_payload)
        cleaned_data.update(request_kwargs)
        return cleaned_data
    else:
        return response


def fetch_single_swap_quote_from_internal_api(
    chain_id: int,
    sell_token: str,
    buy_token: str,
    unscaled_amount_in: int,
    system_name: str = "gen3",
    slippage_bps: int = 50,
    include_sources: str = "",
    exclude_sources: str = "Bebop",
    sell_all: bool = True,
    timeout_ms: int = None,
    transfer_to_caller: bool = True,
) -> dict:
    url = "https://swaps-pricing.tokemaklabs.com/swap-quote-v2"

    payload = {
        "chainId": chain_id,
        "systemName": system_name,
        "slippageBps": slippage_bps,
        "taker": DEAD_ADDRESS,
        "sellToken": sell_token,
        "buyToken": buy_token,
        "sellAmount": str(unscaled_amount_in),
        "includeSources": include_sources,
        "excludeSources": exclude_sources,
        "sellAll": sell_all,
        "timeoutMS": str(timeout_ms) if timeout_ms is not None else "",
        "transferToCaller": transfer_to_caller,
    }

    tokemak_response = make_single_request_to_3rd_party(
        request_kwargs={
            "url": url,
            "json": payload,
            "method": "POST",
        }
    )

    clean_response = _process_quote_response(tokemak_response)
    clean_response.update(payload)

    return clean_response


def fetch_many_swap_quotes_from_internal_api(
    quote_requests: list[TokemakQuoteRequest],
) -> pd.DataFrame:
    requests_kwargs = []
    for quote_request in quote_requests:
        json_payload = {
            "chainId": quote_request.chain_id,
            "systemName": "gen3",
            "slippageBps": 50,
            "taker": DEAD_ADDRESS,
            "sellToken": quote_request.token_in,
            "buyToken": quote_request.token_out,
            "sellAmount": str(quote_request.unscaled_amount_in),
            "includeSources": "",
            "excludeSources": "Bebop",
            "sellAll": True,
        }

        requests_kwargs.append(
            {
                "method": "POST",
                "url": "https://swaps-pricing.tokemaklabs.com/swap-quote-v2",
                "json": json_payload,
            }
        )

    raw_responses = make_many_requests_to_3rd_party(
        rate_limit_max_rate=80, rate_limit_time_period=60, requests_kwargs=requests_kwargs
    )
    flat_responses = [_process_quote_response(r) for r in raw_responses]
    return pd.DataFrame.from_records(flat_responses)


def build_quotes_to_fetch(
    chain: ChainData,
    token_out: str,
    reference_quantity: float,
    asset_exposure: dict[str, float],  # must be scaled down
    portion_to_check: list[float],
) -> list[TokemakQuoteRequest]:

    quotes_to_fetch = []
    for token_in, exposure in asset_exposure.items():
        for portion in portion_to_check:
            scaled_down_exposure = exposure * portion
            quotes_to_fetch.append(
                TokemakQuoteRequest(
                    chain=chain, token_in=token_in, token_out=token_out, scaled_amount_in=str(int(scaled_down_exposure))
                )
            )

    for token_in in asset_exposure.keys():
        quotes_to_fetch.append(
            TokemakQuoteRequest(
                chain=chain, token_in=token_in, token_out=token_out, scaled_amount_in=str(int(reference_quantity))
            )
        )

    return quotes_to_fetch


if __name__ == "__main__":
    from mainnet_launch.constants import *

    tokemak_response = fetch_single_swap_quote_from_internal_api(
        chain_id=ETH_CHAIN.chain_id,
        sell_token=WETH(ETH_CHAIN),
        buy_token="0x04C154b66CB340F3Ae24111CC767e0184Ed00Cc6",
        unscaled_amount_in=str(int(1e18)),
    )

    from pprint import pprint

    pprint(tokemak_response)


# def _post_process_quote_df(
#     all_quotes: list[dict],
#     tokens_df: pd.DataFrame,
#     base_asset_decimals: int,
#     sell_token_to_reference_quantity: dict[str, float],
# ) -> pd.DataFrame:

#     quote_df = pd.DataFrame.from_records(all_quotes)
#     quote_df = pd.merge(quote_df, tokens_df, how="left", left_on="sellToken", right_on="token_address")
#     quote_df["buy_amount_norm"] = quote_df.apply(
#         lambda row: int(row["buyAmount"]) / (10**base_asset_decimals) if pd.notna(row["buyAmount"]) else None,
#         axis=1,
#     )
#     quote_df["min_buy_amount_norm"] = quote_df.apply(
#         lambda row: (int(row["minBuyAmount"]) / (10**base_asset_decimals) if pd.notna(row["minBuyAmount"]) else None),
#         axis=1,
#     )
#     quote_df["Sold Quantity"] = quote_df.apply(
#         lambda row: int(row["sellAmount"]) / (10 ** row["decimals"]) if pd.notna(row["sellAmount"]) else None, axis=1
#     )
#     quote_df["token_price"] = quote_df["buy_amount_norm"] / quote_df["Sold Quantity"]
#     quote_df["min_token_price"] = quote_df["min_buy_amount_norm"] / quote_df["Sold Quantity"]
#     quote_df["reference_quantity"] = quote_df["sellToken"].map(sell_token_to_reference_quantity)

#     return quote_df


# def fetch_quotes(
#     chain: ChainData,
#     base_asset: str,
#     base_asset_decimals: int,
#     current_raw_balances: dict[str, int],
# ) -> tuple[pd.DataFrame, pd.DataFrame]:
#     """
#     Fetch quotes for the given balances and chain.
#     Returns a DataFrame with quotes and a DataFrame with slippage data.
#     """
#     progress_bar = st.progress(0, text="Fetching quotes...")

#     quote_df, slippage_df = run_async_safely(
#         fetch_quotes_OLD(chain, base_asset, base_asset_decimals, current_raw_balances)
#     )

#     progress_bar.empty()
#     return quote_df, slippage_df


# async def fetch_quotes_OLD(
#     chain: ChainData,
#     base_asset: str,
#     base_asset_decimals: int,
#     current_raw_balances: dict[str, int],
# ) -> pd.DataFrame:
#     """
#     Note this is not exact, because of latency in the solver

#     Even if I ask for a bunch of quotes all at time t,

#     the blocks might change between them so the quotes can be slightly different.

#     This should be thought of as an approximation not an exact answer.
#     """

#     tokens_df = get_full_table_as_df(Tokens, where_clause=Tokens.chain_id == chain.chain_id)

#     progress_bar = st.progress(0, text="Fetching quotes...")
#     token_to_decimals = tokens_df.set_index("token_address")["decimals"].to_dict()

#     if base_asset in WETH:
#         total_needed_quotes = len(current_raw_balances.keys()) * (len(PORTIONS_TO_CHECK) + 1) * ATTEMPTS
#     elif (base_asset in USDC) or (base_asset in DOLA):
#         # + 3 is for the constant stable coin amounts
#         total_needed_quotes = len(current_raw_balances.keys()) * (len(PORTIONS_TO_CHECK) + 1 + 3) * ATTEMPTS

#     all_quotes = []
#     async with aiohttp.ClientSession() as session:
#         for attempt in range(ATTEMPTS):
#             tasks = []
#             sell_token_to_reference_quantity = {}
#             if attempt > 0:
#                 st.write(f"sleeping for {12 * (attempt)} seconds to avoid rate limits")
#                 time.sleep(12 * (attempt))
#             for sell_token_address, raw_amount in current_raw_balances.items():

#                 amounts_to_check = [int(raw_amount * portion) for portion in PORTIONS_TO_CHECK]
#                 if base_asset in WETH:
#                     sell_token_to_reference_quantity[sell_token_address] = ETH_REFERENCE_QUANTITY
#                     amounts_to_check.append(5e18)
#                 elif (base_asset in USDC) or (base_asset in DOLA):
#                     reference_quantity = STABLE_COINS_REFERENCE_QUANTITY * (10 ** token_to_decimals[sell_token_address])
#                     sell_token_to_reference_quantity[sell_token_address] = STABLE_COINS_REFERENCE_QUANTITY
#                     amounts_to_check.append(reference_quantity)

#                     # for stable coins also add these checks for constants
#                     for constant_stable_coin_amounts in [50_000, 100_000, 200_000]:
#                         amounts_to_check.append(
#                             constant_stable_coin_amounts * (10 ** token_to_decimals[sell_token_address])
#                         )
#                 else:
#                     raise ValueError(
#                         f"{base_asset=} is not a stable coin or ETH, "
#                         f"so we cannot use it to compute the reference quantity for {sell_token_address}"
#                     )

#                 for scaled_sell_raw_amount in amounts_to_check:

#                     def make_rate_limited_fetch(session, chain_id, sell_token, buy_token, sell_amount):
#                         # this inner function is needed to avoid
#                         # RuntimeError:
#                         # <asyncio.locks.Semaphore object at 0x12d65c250 [locked]> is bound to a different event loop

#                         async def _inner():
#                             async with tokemak_swap_quote_api_rate_limit:
#                                 return await fetch_swap_quote(
#                                     session=session,
#                                     chain_id=chain_id,
#                                     sell_token=sell_token,
#                                     buy_token=buy_token,
#                                     sell_amount=sell_amount,
#                                 )

#                         return _inner()

#                     task = make_rate_limited_fetch(
#                         session=session,
#                         chain_id=chain.chain_id,
#                         sell_token=sell_token_address,
#                         buy_token=base_asset,
#                         sell_amount=scaled_sell_raw_amount,
#                     )
#                     tasks.append(task)

#             for future in asyncio.as_completed(tasks):
#                 quote = await future
#                 all_quotes.append(quote)
#                 portion_done = len(all_quotes) / total_needed_quotes
#                 portion_done = 1 if portion_done > 1 else portion_done
#                 progress_bar.progress(portion_done, text=f"Fetched quotes: {len(all_quotes)}/{total_needed_quotes}")

#     quote_df = _post_process_quote_df(all_quotes, tokens_df, base_asset_decimals, sell_token_to_reference_quantity)
#     slippage_df = compute_excess_slippage_from_size(quote_df)

#     return quote_df, slippage_df


# def compute_excess_slippage_from_size(quote_df: pd.DataFrame) -> pd.DataFrame:
#     # note, this is in
#     # todo add min_buy_amount_ratio
#     slippage_df = (
#         quote_df.groupby(["symbol", "Sold Quantity"])[["buy_amount_norm", "token_price", "reference_quantity"]]
#         .median()
#         .reset_index()
#     )

#     token_price_at_reference_quantity = (
#         slippage_df[slippage_df["reference_quantity"] == slippage_df["Sold Quantity"].astype(int)]
#         .set_index("symbol")["token_price"]
#         .to_dict()
#     )

#     slippage_df["token_price_at_reference_quantity"] = slippage_df["symbol"].map(token_price_at_reference_quantity)

#     highest_sold_amount = slippage_df.groupby("symbol")["Sold Quantity"].max().to_dict()

#     slippage_df["highest_sold_amount"] = slippage_df["symbol"].map(highest_sold_amount)

#     slippage_df["percent_sold"] = slippage_df.apply(
#         lambda row: round(100 * row["Sold Quantity"] / row["highest_sold_amount"], 2), axis=1
#     )
#     slippage_df["bps_loss_excess_vs_reference_price"] = slippage_df.apply(
#         lambda row: 10_000 * (row["token_price_at_reference_quantity"] - row["token_price"]) / row["token_price"],
#         axis=1,
#     )
#     return slippage_df
