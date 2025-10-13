from mainnet_launch.database.schema.ensure_tables_are_current.using_3rd_party.swap_matrix_quotes_and_onchain_prices import (
    build_quotes,
    build_fetch_on_chain_spot_prices_function,
    fetch_swap_matrix_quotes_and_prices,
    _build_all_tokemak_quote_requests,
)
from mainnet_launch.constants import *

import time
import random
import pandas as pd
from multicall import Call

from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor


from mainnet_launch.database.postgres_operations import (
    _exec_sql_and_cache,
)

from mainnet_launch.database.schema.ensure_tables_are_current.using_onchain.helpers.update_blocks import (
    get_block_by_timestamp_alchemy,
)

from mainnet_launch.data_fetching.internal.fetch_quotes import (
    fetch_single_swap_quote_from_internal_api,
    TokemakQuoteRequest,
    THIRD_PARTY_SUCCESS_KEY,
)

from mainnet_launch.data_fetching.get_state_by_block import (
    safe_normalize_6_with_bool_success,
    get_state_by_one_block,
    safe_normalize_with_bool_success,
)


if __name__ == "__main__":
    unique_tokemak_quote_requests = _build_all_tokemak_quote_requests()

    plasma_requests = [r for r in unique_tokemak_quote_requests if r.chain_id == PLASMA_CHAIN  .chain_id]
    plasma_requests = plasma_requests[:5]

    autopool_to_fetch_on_chain_spot_prices_function = {
        autopool: build_fetch_on_chain_spot_prices_function(autopool) for autopool in ALL_AUTOPOOLS
    }

    def process_request(tokemak_quote_request: TokemakQuoteRequest) -> dict:
        this_autopool: AutopoolConstants = tokemak_quote_request.associated_autopool
        data = fetch_swap_matrix_quotes_and_prices(
            autopool_to_fetch_on_chain_spot_prices_function[tokemak_quote_request.associated_autopool],
            tokemak_quote_request,
        )
        data["autopool_name"] = this_autopool.name
        return data

    max_workers = 1
    with ThreadPoolExecutor(max_workers=1) as executor:
        quote_responses = list(
            tqdm(
                executor.map(process_request, plasma_requests),
                total=len(plasma_requests),
                desc=f"Fetching quotes tokemak quote requests",
            )
        )

    all_quote_responses_df = pd.DataFrame.from_records(quote_responses)
    pass

"""

curl -sS -X POST 'https://swaps-pricing.tokemaklabs.com/swap-quote-v2' \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json' \
  --data '{
    "chainId": 42161,
    "systemName": "gen3",
    "slippageBps": 5000,
    "taker": "0x000000000000000000000000000000000000dEaD",
    "sellToken": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
    "buyToken": "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9", 
    "sellAmount": 50000000000,
    "includeSources": "",
    "excludeSources": "Bebop,Lifi",
    "sellAll": true,
    "timeoutMS": 20000,
    "returnAll": true
  }'

undefined%


"""