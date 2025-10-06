from mainnet_launch.database.schema.ensure_tables_are_current.using_3rd_party.swap_matrix_quotes_and_onchain_prices import (
    build_quotes,
    build_fetch_on_chain_spot_prices_function, 
    fetch_swap_matrix_quotes_and_prices
)
from mainnet_launch.constants import PLASMA_USD, ARB_USD


tokemak_quote_requests = build_quotes(PLASMA_USD)


requests = [a for a in tokemak_quote_requests if  a.token_out == PLASMA_USD.base_asset]

# tokemak_quote_requests = sorted(tokemak_quote_requests, key=lambda x: x.scaled_amount_in)
_fetch_on_chain_spot_prices = build_fetch_on_chain_spot_prices_function(PLASMA_USD)

a = fetch_swap_matrix_quotes_and_prices(_fetch_on_chain_spot_prices, requests[0])





# plasma fails if I exclude bebop and lifi
# otherwise it succeeds
# gho = '0x7dfF72693f6A4149b17e7C6314655f6A9F7c8B33'
# usdc = '0xaf88d065e77c8cC2239327C5EDb3A432268e5831'
# GHO -> USDC fails with this

# { 'json': { 'buyToken': '0xaf88d065e77c8cC2239327C5EDb3A432268e5831',
#             'chainId': 42161,
#             'excludeSources': 'Bebop,Lifi',
#             'includeSources': '',
#             'sellAll': True,
#             'sellAmount': '50000000000000000000000',
#             'sellToken': '0x7dfF72693f6A4149b17e7C6314655f6A9F7c8B33',
#             'slippageBps': 5000,
#             'systemName': 'gen3',
#             'taker': '0x000000000000000000000000000000000000dEaD',
#             'timeoutMS': 20000},
#   'method': 'POST',
#   'url': 'https://swaps-pricing.tokemaklabs.com/swap-quote-v2'}

# can't find swaps for 50k GHO -> USDC on arb get the no swaps found error

# https://app.1inch.io/swap?src=42161:GHO&dst=42161:USDC
# there is liquidity via 1inch, but our setup fails