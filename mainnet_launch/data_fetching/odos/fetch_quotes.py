from dataclasses import dataclass
import asyncio
import aiohttp
from typing import Optional, Sequence, Iterable
import pandas as pd

from mainnet_launch.constants.constants import *
from mainnet_launch.data_fetching.fetch_data_from_3rd_party_api import (
    make_single_request_to_3rd_party,
    make_many_requests_to_3rd_party,
    THIRD_PARTY_SUCCESS_KEY,
)
from pprint import pprint

# add a configurable parameter for what level of exposrue to exclude
# eg we can still trade witha pool where we ahve 5% of it
# start with a 10% threshold
# do this later

ODOS_RATELIMIT_MAX_RATE = 600  #  # 600 requests per 5 minutes
ODOS_RATE_LIMIT_TIME_PERIOD = 5 * 60  # 5 minutes

ODOS_BASE_URL = "https://api.odos.xyz"


@dataclass
class OdosQuoteRequest:
    chain_id: int
    token_in: str
    token_out: str
    unscaled_amount_in: str
    poolBlacklist: Optional[tuple[str]] = None


def fetch_odos_single_token_raw_quote(
    chain_id: int, token_in: str, token_out: str, unscaled_amount_in: str, poolBlacklist: tuple[str] = None
) -> dict:
    # https://docs.odos.xyz/build/api-docs

    json_payload = {
        "chainId": chain_id,
        "inputTokens": [{"tokenAddress": token_in, "amount": unscaled_amount_in}],
        "outputTokens": [{"tokenAddress": token_out, "proportion": 1.0}],
        "compact": False,
        "simple": False,
        "likeAsset": True,
    }

    if poolBlacklist:
        json_payload["poolBlacklist"] = poolBlacklist

    request_kwargs = {
        "method": "POST",
        "url": f"{ODOS_BASE_URL}/sor/quote/v3",
        "json": json_payload,
    }
    raw_odos_response = make_single_request_to_3rd_party(request_kwargs=request_kwargs)

    flat_odos_response = _flatten_odos_response(raw_odos_response)
    return flat_odos_response


def fetch_many_odos_raw_quotes(quote_requests: list[OdosQuoteRequest]) -> pd.DataFrame:
    requests_kwargs = []
    for quote_request in quote_requests:

        json_payload = {
            "chainId": quote_request.chain_id,
            "inputTokens": [{"tokenAddress": quote_request.token_in, "amount": str(quote_request.unscaled_amount_in)}],
            "outputTokens": [{"tokenAddress": quote_request.token_out, "proportion": 1.0}],
            "compact": False,
            "simple": False,
        }
        if quote_request.poolBlacklist:
            json_payload["poolBlacklist"] = quote_request.poolBlacklist

        requests_kwargs.append(
            {
                "method": "POST",
                "url": f"{ODOS_BASE_URL}/sor/quote/v3",
                "json": json_payload,
            }
        )

    # slightly under the 600 / 5 minute
    # interestingly this does not preserve order
    raw_odos_responses = make_many_requests_to_3rd_party(
        rate_limit_max_rate=8, rate_limit_time_period=10, requests_kwargs=requests_kwargs
    )

    flat_odos_responses = [_flatten_odos_response(r) for r in raw_odos_responses]

    df = pd.DataFrame.from_records(flat_odos_responses)
    return df


def _flatten_odos_response(raw_odos_response: dict):
    if raw_odos_response[THIRD_PARTY_SUCCESS_KEY]:

        keys_to_keep = [
            "inTokens",
            "inAmounts",
            "outTokens",
            "outAmounts",
            "datetime_received",
            THIRD_PARTY_SUCCESS_KEY,
        ]
        flat_odos_data = {}

        for col in keys_to_keep:
            value = raw_odos_response[col]
            if isinstance(value, list) and len(value) == 1:
                # if the value is a list with one item, take that item
                flat_odos_data[col] = value[0]
            elif isinstance(value, list) and len(value) > 1:
                raise ValueError(
                    f"Expected {col} to have one item, but got {len(value)} items: {value}. "
                    "This is unexpected from odos response."
                )
            else:
                flat_odos_data[col] = value

    else:
        # if the request was not successful, return an empty dict

        flat_odos_data = {
            THIRD_PARTY_SUCCESS_KEY: raw_odos_response[THIRD_PARTY_SUCCESS_KEY],
        }
    request_kwargs = raw_odos_response.pop(
        "request_kwargs",
    )
    flat_odos_data.update(request_kwargs.pop("json"))
    flat_odos_data.update(request_kwargs)
    return flat_odos_data


if __name__ == "__main__":

    quote_response = fetch_odos_single_token_raw_quote(
        chain_id=ETH_CHAIN.chain_id,
        token_in=WETH(ETH_CHAIN),
        token_out="0x04C154b66CB340F3Ae24111CC767e0184Ed00Cc6",
        unscaled_amount_in=str(int(1e18)),
    )

    pprint(quote_response)

    quote_response = fetch_odos_single_token_raw_quote(
        chain_id=ETH_CHAIN.chain_id,
        token_in=DEAD_ADDRESS,
        token_out="0x04C154b66CB340F3Ae24111CC767e0184Ed00Cc6",
        unscaled_amount_in=str(int(1e18)),
    )

    pprint(quote_response)
    pass
