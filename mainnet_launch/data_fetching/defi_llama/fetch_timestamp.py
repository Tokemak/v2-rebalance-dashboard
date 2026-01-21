from enum import Enum

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
    make_single_request_to_3rd_party,
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


def get_block_by_timestamp_defi_llama(unix_timestamp: int, chain: ChainData, closest: str) -> int:
    if closest not in (Closest.BEFORE, Closest.AFTER):
        raise DeFiLlamaAPIError(f"closest must be 'before' or 'after', got: {closest}")

    chain_slug = CHAIN_TO_DEFI_LLAMA_SLUG[chain]
    url = f"https://coins.llama.fi/block/{chain_slug}/{unix_timestamp}"

    data = make_single_request_to_3rd_party({"method": "GET", "url": url})

    if not data.get(THIRD_PARTY_SUCCESS_KEY):
        raise ThirdPartyAPIError("DeFi Llama request failed", data)
    try:
        height = int(data["height"])
    except Exception as e:
        raise ThirdPartyAPIError(f"Unexpected DeFi Llama response: {type(e).__name__}: {e}", data)

    return height + 1 if closest == "after" else height


def fetch_blocks_by_unix_timestamps_defillama(
    unix_timestamps: list[int],
    chain: ChainData,
    rate_limit_max_rate: int = 5,
    rate_limit_time_period: int = 1,
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
        requests_kwargs.append(_defillama_block_request_kwargs(ts - 10, Closest.BEFORE))
        requests_kwargs.append(_defillama_block_request_kwargs(ts + 10, Closest.BEFORE))
        requests_kwargs.append(_defillama_block_request_kwargs(ts + 86400 + 10, Closest.BEFORE))

    responses = make_many_requests_to_3rd_party(
        rate_limit_max_rate=rate_limit_max_rate,
        rate_limit_time_period=rate_limit_time_period,
        requests_kwargs=requests_kwargs,
    )

    import pandas as pd

    response_df = pd.DataFrame(responses)
    request_df = pd.json_normalize(requests_kwargs)

    joined_df = pd.concat([request_df, response_df], axis=1)
    joined_df["pdtimestamp"] = pd.to_datetime(joined_df["timestamp"], unit="s", utc=True)
    # print(joined_df[['pdtimestamp', 'height', 'timestamp']].head(20))
    joined_df = joined_df.dropna(subset=["height"])

    blocks_add = list(joined_df["height"].astype(int))
    return blocks_add
