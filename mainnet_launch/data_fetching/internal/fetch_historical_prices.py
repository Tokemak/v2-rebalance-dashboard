"""
Also lets you get prices from timestamps in the past

Don't rely on this for perfect accuracy, it is using coingecko, etc for past prices

"""

from __future__ import annotations


from dataclasses import dataclass

import pandas as pd
from web3 import Web3

from mainnet_launch.data_fetching.fetch_data_from_3rd_party_api import (
    make_many_requests_to_3rd_party,
    make_single_request_to_3rd_party,
    THIRD_PARTY_SUCCESS_KEY,
)


@dataclass
class TokemakPriceRequest:
    chain_id: int
    token_to_price: str
    denominate_in: str
    denominate_in_decimals: int
    timestamp: int

    def __post_init__(self):
        self.denominate_in = Web3.toChecksumAddress(self.denominate_in)
        self.token_to_price = Web3.toChecksumAddress(self.token_to_price)


def _build_price_request_kwargs(
    chain_id: int, token_to_price: str, timestamp: int, denominate_in: str, denominate_in_decimals: int
):
    params = {
        "chainId": chain_id,
        "systemName": "gen3",
        "token": token_to_price,
        "denominateIn": denominate_in,
        "timestamp": timestamp,
        "timeoutMS": 5000,
    }

    if denominate_in and denominate_in != "0x0000000000000000000000000000000000000000":
        params["denominateIn"] = denominate_in
        if denominate_in_decimals is not None:
            params["denominateInDecimals"] = denominate_in_decimals

    return {
        "method": "GET",
        "url": "https://generic-swaps-prices-infra-staging.tokemak.workers.dev/price",
        "params": params,
    }


def _process_price_response(response: dict) -> dict:
    if response.get(THIRD_PARTY_SUCCESS_KEY) and "price" in response:
        cleaned_data = {
            "price": response["price"],
            "datetime_received": response.get("datetime_received"),
            THIRD_PARTY_SUCCESS_KEY: response[THIRD_PARTY_SUCCESS_KEY],
        }
        cleaned_data.update(response.get("request_kwargs", {}))
        return cleaned_data
    return response


def fetch_many_prices_from_internal_api(
    price_requests: list[TokemakPriceRequest], rate_limit_max_rate: int = 10, rate_limit_time_period: int = 10
) -> pd.DataFrame:
    requests_kwargs = [
        _build_price_request_kwargs(
            req.chain_id,
            req.token_to_price,
            req.timestamp,
            req.denominate_in,
            req.denominate_in_decimals,
        )
        for req in price_requests
    ]

    raw_responses = make_many_requests_to_3rd_party(
        rate_limit_max_rate=rate_limit_max_rate,
        rate_limit_time_period=rate_limit_time_period,
        requests_kwargs=requests_kwargs,
    )

    flat_responses = [_process_price_response(r) for r in raw_responses]
    df = pd.json_normalize(flat_responses)
    return df


def fetch_single_price_from_internal_api(
    chain_id: int,
    token_to_price: str,
    timestamp: int,
    denominate_in: str | None = None,
    denominate_in_decimals: int | None = None,
) -> dict:
    request_kwargs = _build_price_request_kwargs(
        chain_id, token_to_price, timestamp, denominate_in, denominate_in_decimals
    )
    tokemak_response = make_single_request_to_3rd_party(request_kwargs=request_kwargs)
    return _process_price_response(tokemak_response)


if __name__ == "__main__":
    from pprint import pprint
    from mainnet_launch.constants import ETH_CHAIN, WETH, USDC, ALL_CHAINS

    requests = []

    for chain in ALL_CHAINS:
        requests.append(
            TokemakPriceRequest(
                chain_id=chain.chain_id,
                token_to_price=WETH(chain),
                denominate_in=USDC(chain),
                denominate_in_decimals=6,
                timestamp=1756487446 - 100,
            )
        )

    df = fetch_many_prices_from_internal_api(requests)

    pprint(df)
