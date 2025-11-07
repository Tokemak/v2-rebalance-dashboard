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


def build_get_token_prices_by_address_request(
    chain: ChainData,
    token_addresses: list[str],
) -> dict:
    response = make_single_request_to_3rd_party(
        request_kwargs={
            "method": "POST",
            "url": f"https://api.g.alchemy.com/prices/v1/{ALCHEMY_API_KEY}/tokens/by-address",
            "json": {"addresses": [{"network": chain.alchemy_network_enum, "address": a} for a in token_addresses]},
            "headers": {"Content-Type": "application/json", "Accept": "application/json"},
        },
        custom_failure_function=None,
    )

    def _extract_price_info_from_token_data(data: list[dict]):
        token_prices = {}
        for token_info in data:
            address = token_info["address"]
            if len(token_info["prices"]) == 0:
                usd_price = None
            else:
                usd_price = token_info["prices"][0]["value"]
            token_prices[address] = usd_price
        return token_prices

    if response[THIRD_PARTY_SUCCESS_KEY]:
        token_prices = _extract_price_info_from_token_data(response["data"])
        return token_prices
    else:
        return response["data"]


if __name__ == "__main__":
    from pprint import pprint

    treasury_wallet = "0x8b4334d4812c530574bd4f2763fcd22de94a969b"

    for autopool in ALL_AUTOPOOLS:
        data = build_get_token_prices_by_address_request(
            chain=autopool.chain,
            token_addresses=[autopool.base_asset],
        )
        if THIRD_PARTY_SUCCESS_KEY in data:
            pprint(f"Failed to get token prices for {autopool.name} on {autopool.chain.name}")
            pprint(data)
        else:
            pprint(f"Token prices for {autopool.name} on {autopool.chain.name}:")
            pprint(data)
