from dataclasses import dataclass

import pandas as pd


from mainnet_launch.constants import DEAD_ADDRESS, ChainData, AutopoolConstants
from mainnet_launch.data_fetching.fetch_data_from_3rd_party_api import (
    make_many_requests_to_3rd_party,
    make_single_request_to_3rd_party,
    THIRD_PARTY_SUCCESS_KEY,
)


@dataclass
class TokemakQuoteRequest:
    chain_id: int
    token_in: str
    token_out: str
    unscaled_amount_in: str

    includeSources: str = ""
    excludeSources: str = "Bebop,Lifi"
    slippageBps: int = 5000

    associated_autopool: AutopoolConstants = None


def fetch_single_swap_quote_from_internal_api(
    tokemak_quote_request: TokemakQuoteRequest,
) -> dict:
    request_kwargs = _build_request_kwargs(tokemak_quote_request)
    tokemak_response = make_single_request_to_3rd_party(request_kwargs=request_kwargs)
    clean_response = _process_quote_response(tokemak_response)
    return clean_response


def _build_request_kwargs(tokemak_quote_request: TokemakQuoteRequest) -> dict:
    json_payload = {
        "chainId": tokemak_quote_request.chain_id,
        "systemName": "gen3",
        "slippageBps": tokemak_quote_request.slippageBps,
        "taker": DEAD_ADDRESS,
        "sellToken": tokemak_quote_request.token_in,
        "buyToken": tokemak_quote_request.token_out,
        "sellAmount": tokemak_quote_request.unscaled_amount_in,
        "includeSources": tokemak_quote_request.includeSources,
        "excludeSources": tokemak_quote_request.excludeSources,
        "sellAll": True,
        "timeoutMS": 20000,
    }

    requests_kwargs = {
        "method": "POST",
        "url": "https://swaps-pricing.tokemaklabs.com/swap-quote-v2",
        "json": json_payload,
    }
    return requests_kwargs


def _process_quote_response(response: dict) -> dict:
    if response[THIRD_PARTY_SUCCESS_KEY]:
        fields_to_keep = ["buyAmount", "minBuyAmount", "aggregatorName", "datetime_received", THIRD_PARTY_SUCCESS_KEY]
        cleaned_data = {a: response[a] for a in fields_to_keep}
        request_kwargs = response["request_kwargs"]
        json_payload = request_kwargs.pop("json")
        cleaned_data.update(json_payload)
        cleaned_data.update(request_kwargs)
        return cleaned_data
    else:
        fields_to_keep = ["buyAmount", "minBuyAmount", "aggregatorName", "datetime_received", THIRD_PARTY_SUCCESS_KEY]
        cleaned_data = {a: None for a in fields_to_keep}
        cleaned_data[THIRD_PARTY_SUCCESS_KEY] = False
        request_kwargs = response["request_kwargs"]
        json_payload = request_kwargs.pop("json")
        cleaned_data.update(json_payload)
        cleaned_data.update(request_kwargs)
        return cleaned_data


def fetch_many_swap_quotes_from_internal_api(
    quote_requests: list[TokemakQuoteRequest], rate_limit_max_rate: int = 8, rate_limit_time_period: int = 10
) -> pd.DataFrame:
    requests_kwargs = []
    for tokemak_quote_request in quote_requests:
        requests_kwargs.append(_build_request_kwargs(tokemak_quote_request=tokemak_quote_request))

    # no real idea here
    # 1-2  per second
    # not really sure here
    raw_responses = make_many_requests_to_3rd_party(
        rate_limit_max_rate=rate_limit_max_rate,
        rate_limit_time_period=rate_limit_time_period,
        requests_kwargs=requests_kwargs,
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
        unscaled_amount_in=str(int(10000e18)),
    )

    from pprint import pprint

    pprint(tokemak_response)
    pass
