"""
see https://www.alchemy.com/docs/data/prices-api/prices-api-endpoints/prices-api-endpoints/get-token-prices-by-address

Note: does not work for sonic or plasma as of Oct 29, 2025

"""

from mainnet_launch.constants import ChainData, ALCHEMY_API_KEY, ALL_CHAINS, ALL_AUTOPOOLS
from mainnet_launch.data_fetching.fetch_data_from_3rd_party_api import (
    make_single_request_to_3rd_party,
    make_many_requests_to_3rd_party,
    make_naive_get_request,
    make_naive_post_request,
    THIRD_PARTY_SUCCESS_KEY,
)

from mainnet_launch.database.views import get_token_details_dict


def get_token_prices_from_token_symbol_endpoint_alchemy(
    token_addresses: list[str],
) -> dict:

    if len(token_addresses) > 25:
        raise ValueError("Alchemy API only supports up to 25 addresses per request")

    token_to_decimals, token_to_symbol = get_token_details_dict()
    symbols_list = [token_to_symbol[a] for a in token_addresses]

    response = make_single_request_to_3rd_party(
        request_kwargs={
            "method": "GET",
            "url": f"https://api.g.alchemy.com/prices/v1/{ALCHEMY_API_KEY}/tokens/by-symbol",
            "params": {"symbols": symbols_list},
        },
        custom_failure_function=None,
    )

    def _extract_price_info_from_token_data(data: list[dict]):
        token_prices = {}
        for token_info in data:
            symbol = token_info["symbol"]
            if len(token_info["prices"]) == 0:
                usd_price = None
            else:
                usd_price = float(token_info["prices"][0]["value"])
            token_prices[symbol] = usd_price
        return token_prices

    if response[THIRD_PARTY_SUCCESS_KEY]:
        token_prices = _extract_price_info_from_token_data(response["data"])

        symbol_to_address = {v: k for k, v in token_to_symbol.items()}
        token_prices = {symbol_to_address[symbol]: price for symbol, price in token_prices.items()}

        return token_prices
    else:
        return response["data"]


if __name__ == "__main__":

    from pprint import pprint

    for autopool in ALL_AUTOPOOLS:
        data = get_token_prices_from_token_symbol_endpoint_alchemy(
            token_addresses=[autopool.base_asset],
        )

        pprint(data)
