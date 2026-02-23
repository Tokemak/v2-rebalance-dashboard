from enum import Enum
import pandas as pd

from mainnet_launch.constants import (
    ETH_CHAIN,
    BASE_CHAIN,
    SONIC_CHAIN,
    ARBITRUM_CHAIN,
    PLASMA_CHAIN,
    LINEA_CHAIN,
    ChainData,
)
from mainnet_launch.data_fetching.fetch_data_from_3rd_party_api import (
    THIRD_PARTY_SUCCESS_KEY,
    ThirdPartyAPIError,
    make_many_requests_to_3rd_party,
)


CHAIN_TO_DEFI_LLAMA_SLUG = {
    ETH_CHAIN: "ethereum",
    BASE_CHAIN: "base",
    SONIC_CHAIN: "sonic",
    ARBITRUM_CHAIN: "arbitrum",
    PLASMA_CHAIN: "plasma",
    LINEA_CHAIN: "linea",  # not tested
}


class Closest(str, Enum):
    BEFORE = "before"
    AFTER = "after"


class DeFiLlamaAPIError(ThirdPartyAPIError):
    pass


def fetch_blocks_by_unix_timestamps_defillama(
    unix_timestamps: list[int],
    chain: ChainData,
    rate_limit_max_rate: int = 5,
    rate_limit_time_period: int = 2,
):
    """
    Fetch blocks for a list of unix timestamps on a given chain via DeFiLlama,
    using the existing async/rate-limited request helper.

    Returns:
        blocks_to_add: set[int]
        failures: list[dict]  (each contains timestamp + response for inspection)


    gets the closest block before AND after the timestamp
    """
    # note assumes all timestamps are second 0 on the day Mon Jan 19 2026 00:00:00 GMT+0000
    if not unix_timestamps:
        raise DeFiLlamaAPIError("No unix_timestamps provided")

    def _defillama_block_request_kwargs(ts: int, closest: str):
        url = f"https://coins.llama.fi/block/{CHAIN_TO_DEFI_LLAMA_SLUG[chain]}/{int(ts)}"
        params = {"closest": closest}
        return {"method": "GET", "url": url, "params": params}

    requests_kwargs = []
    for ts in unix_timestamps:
        requests_kwargs.append(_defillama_block_request_kwargs(ts, Closest.BEFORE))
    # this should retry and try again on 500 errors.
    responses = make_many_requests_to_3rd_party(
        rate_limit_max_rate=rate_limit_max_rate,
        rate_limit_time_period=rate_limit_time_period,
        requests_kwargs=requests_kwargs,
    )

    response_df = pd.DataFrame(responses)
    request_df = pd.json_normalize(requests_kwargs)

    joined_df = pd.concat([request_df, response_df], axis=1)
    joined_df["pdtimestamp"] = pd.to_datetime(joined_df["timestamp"], unit="s", utc=True)
    joined_df = joined_df.dropna(subset=["height"])

    blocks_add = list(joined_df["height"].astype(int))
    return blocks_add
