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

    arb_requests = [r for r in unique_tokemak_quote_requests if r.chain_id == ARBITRUM_CHAIN.chain_id]
    arb_requests = arb_requests[:5]

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
                executor.map(process_request, arb_requests),
                total=len(arb_requests),
                desc=f"Fetching quotes tokemak quote requests",
            )
        )

    all_quote_responses_df = pd.DataFrame.from_records(quote_responses)

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

# usdc  on arb 0xaf88d065e77c8cC2239327C5EDb3A432268e5831


# a balancer pool we support on arbitrum
# [[0x7F6501d3B98eE91f9b9535E4b0ac710Fb0f9e0bc]
# [0xa6D12574eFB239FC1D2099732bd8b5dC6306897F]
# [0xD089B4cb88Dacf4e27be869A00e9f7e2E3C18193]]


# 0x87d60A3b39658842dc75c1C65E0870F1131c4f02 new taker (random address)


# why does this wokr, but the other doesn't
"""

curl -sS -X POST 'https://swaps-pricing.tokemaklabs.com/swap-quote-v2' \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json' \
  --data '{
    "chainId": 42161,
    "systemName": "gen3",
    "slippageBps": 5000,
    "taker": "0x000000000000000000000000000000000000dEaD",
    "sellToken": "0x7F6501d3B98eE91f9b9535E4b0ac710Fb0f9e0bc",
    "buyToken": "0xa6D12574eFB239FC1D2099732bd8b5dC6306897F",
    "sellAmount": 1000000,
    "sellAll": true,
    "timeoutMS": 20000
  }'

works

"""


"""


curl -sS -X POST 'https://swaps-pricing.tokemaklabs.com/swap-quote-v2' \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json' \
  --data '{
    "chainId": 42161,
    "systemName": "gen3",
    "slippageBps": 5000,
    "taker": "0x87d60A3b39658842dc75c1C65E0870F1131c4f02",
    "sellToken": "0x7F6501d3B98eE91f9b9535E4b0ac710Fb0f9e0bc",
    "buyToken": "0xa6D12574eFB239FC1D2099732bd8b5dC6306897F",
    "sellAmount": 1000000,
    "sellAll": true,
    "timeoutMS": 20000
  }'

works



"""


# sell uSDC works


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
    "sellAmount": 1000000,
    "sellAll": true,
    "timeoutMS": 20000
  }'
  
  
  """

# not sure why not but we can't buy this 0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9

# undefined%


#    chain_id                               token_address  symbol                     name  decimals
# 0     42161  0xaf88d065e77c8cC2239327C5EDb3A432268e5831    USDC                 USD Coin         6
# 1     42161  0x7dfF72693f6A4149b17e7C6314655f6A9F7c8B33     GHO                Gho Token        18
# 2     42161  0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9   USD₮0                    USD₮0         6
# 3     42161  0x498Bf2B1e120FeD3ad3D42EA2165E9b73f99C1e5  crvUSD  Curve.Fi USD Stablecoin        18


arb_USDC = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
arb_GHO = "0x7dfF72693f6A4149b17e7C6314655f6A9F7c8B33"
arb_USDT0 = "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9"
arb_crvUSD = "0x498Bf2B1e120FeD3ad3D42EA2165E9b73f99C1e5"

arb_assets = [arb_USDC, arb_GHO, arb_USDT0, arb_crvUSD]

for (b,) in arb_assets:
    for s in arb_assets:
        if b != s:
            print("buy", b, "sell", s)

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
"""


## testing


"""

curl -sS -X POST 'https://swaps-pricing.tokemaklabs.com/swap-quote-v2' \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json' \
  --data '{
    "chainId": 42161,
    "systemName": "gen3",
    "slippageBps": 5000,
    "taker": "0x000000000000000000000000000000000000dEaD",
    "sellToken": "0x7F6501d3B98eE91f9b9535E4b0ac710Fb0f9e0bc",
    "buyToken": "0xa6D12574eFB239FC1D2099732bd8b5dC6306897F",
    "sellAmount": 50000000000,
    "includeSources": "",
    "excludeSources": "Bebop,Lifi",
    "sellAll": true,
    "timeoutMS": 20000,
    "returnAll": true
  }'

works

"""


# not sure 0xaf88d065e77c8cC2239327C5EDb3A432268e5831

# fails  idk why selling GHO for USDC? what is going on here?
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
    "buyToken": "0x7dfF72693f6A4149b17e7C6314655f6A9F7c8B33",
    "sellAmount": 50000000000,
    "includeSources": "",
    "excludeSources": "",
    "sellAll": true,
    "timeoutMS": 20000,
    "returnAll": true
  }'

FAILS"""

# sell USDC  -> GHO


# also fails
# curl -sS -X POST 'https://swaps-pricing.tokemaklabs.com/swap-quote-v2' \
#   -H 'Content-Type: application/json' \
#   -H 'Accept: application/json' \
#   --data '{
#     "chainId": 42161,
#     "systemName": "gen3",
#     "slippageBps": 5000,
#     "taker": "0x000000000000000000000000000000000000dEaD",
#     "sellToken": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
#     "buyToken": "0x7dfF72693f6A4149b17e7C6314655f6A9F7c8B33",
#     "sellAmount": 50000000000,
#     "includeSources": "",
#     "excludeSources": "",
#     "sellAll": true,
#     "timeoutMS": 20000,
#     "returnAll": true
#   }'

# try again in 10 minutes?
# maybe a rate limit? idk
#

# # not sure
# curl -sS -X POST 'https://swaps-pricing.tokemaklabs.com/swap-quote-v2' \
#   -H 'Content-Type: application/json' \
#   -H 'Accept: application/json' \
#   --data '{
#     "chainId": 42161,
#     "systemName": "gen3",
#     "slippageBps": 5000,
#     "taker": "0x000000000000000000000000000000000000dEaD",
#     "sellToken": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
#     "buyToken": "0x7dfF72693f6A4149b17e7C6314655f6A9F7c8B33",
#     "sellAmount": 1000000,
#     "timeoutMS": 20000
#   }'


# failing but with new taker
"""
# don't use the taker address



curl -sS -X POST 'https://swaps-pricing.tokemaklabs.com/swap-quote-v2' \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json' \
  --data '{
    "chainId": 42161,
    "systemName": "gen3",
    "slippageBps": 5000,
    "taker": "0x8b4334d4812c530574bd4f2763fcd22de94a969b",
    "sellToken": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
    "buyToken": "0x498Bf2B1e120FeD3ad3D42EA2165E9b73f99C1e5",
    "sellAmount": 1000000,
    "timeoutMS": 20000
  }'


"""
