from dataclasses import dataclass
import asyncio
import aiohttp
from typing import Optional, Sequence, Iterable
import pandas as pd

from mainnet_launch.constants import *
from mainnet_launch.data_fetching.fetch_data_from_3rd_party_api import (
    make_single_request_to_3rd_party,
    make_many_requests_to_3rd_party,
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
    chain: ChainData
    token_in: str
    token_out: str
    unscaled_amount_in: str
    poolBlacklist: Optional[tuple[str]] = None


def fetch_odos_single_token_raw_quote(
    chain: ChainData, token_in: str, token_out: str, unscaled_amount_in: str, poolBlacklist: tuple[str] = None
) -> dict:
    # https://docs.odos.xyz/build/api-docs

    json_payload = {
        "chainId": chain.chain_id,
        "inputTokens": [{"tokenAddress": token_in, "amount": unscaled_amount_in}],
        "outputTokens": [{"tokenAddress": token_out, "proportion": 1.0}],
        "compact": False,
        "simple": False,
    }

    if poolBlacklist:
        json_payload["poolBlacklist"] = poolBlacklist

    request_kwargs = {
        "method": "POST",
        "url": f"{ODOS_BASE_URL}/sor/quote/v2",
        "json": json_payload,
    }
    raw_odos_response = make_single_request_to_3rd_party(request_kwargs=request_kwargs)

    flat_odos_response = _flatten_odos_response(raw_odos_response)
    return flat_odos_response


def fetch_many_odos_raw_quotes(quote_requests: list[OdosQuoteRequest]) -> dict:
    requests_kwargs = []
    for quote_request in quote_requests:

        json_payload = {
            "chainId": quote_request.chain.chain_id,
            "inputTokens": [{"tokenAddress": quote_request.token_in, "amount": quote_request.unscaled_amount_in}],
            "outputTokens": [{"tokenAddress": quote_request.token_out, "proportion": 1.0}],
            "compact": False,
            "simple": False,
        }
        if quote_request.poolBlacklist:
            json_payload["poolBlacklist"] = quote_request.poolBlacklist

        requests_kwargs.append({
            "method": "POST",
            "url": f"{ODOS_BASE_URL}/sor/quote/v2",
            "json": json_payload,
        })

    # slightly under the 600 / 5 minute
    raw_odos_responses = make_many_requests_to_3rd_party(
        rate_limit_max_rate=100, rate_limit_time_period=60, requests_kwargs=requests_kwargs
    )

    flat_odos_responses = [_flatten_odos_response(r) for r in raw_odos_responses]
    return pd.DataFrame.from_records(flat_odos_responses)


def _flatten_odos_response(raw_odos_response: dict):
    flat_odos_data = {}
    # this works because we only have one input and one output token
    for k, v in raw_odos_response.items():
        if isinstance(v, list):
            flat_odos_data[k] = v[0]
        else:
            flat_odos_data[k] = v
    return flat_odos_data


if __name__ == "__main__":

    quote_response = fetch_odos_single_token_raw_quote(
        chain=ETH_CHAIN,
        token_in=WETH(ETH_CHAIN),
        token_out="0x04C154b66CB340F3Ae24111CC767e0184Ed00Cc6",
        unscaled_amount_in=str(int(1e18)),
    )

    pprint(quote_response)

    quote_response = fetch_odos_single_token_raw_quote(
        chain=ETH_CHAIN,
        token_in=DEAD_ADDRESS,
        token_out="0x04C154b66CB340F3Ae24111CC767e0184Ed00Cc6",
        unscaled_amount_in=str(int(1e18)),
    )

    pprint(quote_response)
    pass
