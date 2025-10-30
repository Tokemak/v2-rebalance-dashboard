# from mainnet_launch.constants import ChainData, ALCHEMY_API_KEY
# from mainnet_launch.data_fetching.fetch_data_from_3rd_party_api import (
#     make_single_request_to_3rd_party,
#     THIRD_PARTY_SUCCESS_KEY,
# )


# def build_get_token_balances_request(
#     chain: ChainData,
#     wallet_address: str,
# ) -> dict:
#     """
#     Calls Alchemy JSON-RPC `alchemy_getTokenBalances` for a specific wallet and list of ERC-20 contracts,
#     returning a dict keyed by token contract address with both hex and parsed integer balances.

#     Notes:
#     - Uses the chain's Alchemy network subdomain (e.g., 'eth-mainnet') plus your ALCHEMY_API_KEY.
#     - If a token isn't held, Alchemy returns '0x00...00'; we decode that to int(0).
#     - Does NOT attempt decimal normalization; downstream can combine with token metadata to scale by 10**decimals.
#     """

#     # Alchemy JSON-RPC base for the chain; e.g. https://eth-mainnet.g.alchemy.com/v2/{API_KEY}
#     rpc_url = chain.client.provider.endpoint_uri

#     response = make_single_request_to_3rd_party(
#         request_kwargs={
#             "method": "POST",
#             "url": rpc_url,
#             "json": {
#                 "jsonrpc": "2.0",
#                 "method": "alchemy_getTokenBalances",
#                 "params": [wallet_address, "erc20"],
#                 "id": 1,
#             },
#             "headers": {},
#         },
#         custom_failure_function=None,
#     )
#     return response

#     # def _hex_to_int(x: str) -> int:
#     #     # Alchemy returns 0x-prefixed hex strings; guard None/empty just in case.
#     #     if not x:
#     #         return 0
#     #     try:
#     #         return int(x, 16)
#     #     except Exception:
#     #         return 0

#     def _extract_balances(result_obj: dict) -> dict:
#         # result_obj structure:
#         # { "address": <wallet>, "tokenBalances": [{ "contractAddress": ..., "tokenBalance": "0x..."}, ...] }
#         out = {}
#         token_balances = (result_obj or {}).get("tokenBalances", [])
#         for tb in token_balances:
#             contract = tb.get("contractAddress")
#             hex_bal = tb.get("tokenBalance")
#             out[contract] = {
#                 "hex": hex_bal,
#                 "int": _hex_to_int(hex_bal),
#             }
#         return out

#     if response.get(THIRD_PARTY_SUCCESS_KEY):
#         result = response.get("data", {}).get("result", {})
#         return _extract_balances(result)
#     else:
#         # Bubble the whole response so caller can log/inspect failure details (keeps your calling pattern intact).
#         return response


# if __name__ == "__main__":
#     from pprint import pprint

#     # Example: Vitalik's wallet, USDC on mainnet
#     wallet = "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045"
#     usdc = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"

#     # You likely have an ETH Mainnet ChainData already in ALL_CHAINS; adjust as needed:
#     # e.g., eth_chain = next(c for c in ALL_CHAINS if c.name == "Ethereum")
#     # For illustration, assume you have `ETH_CHAIN` constant; otherwise pick from ALL_CHAINS.
#     from mainnet_launch.constants import ETH_CHAIN

#     data = build_get_token_balances_request(
#         chain=ETH_CHAIN,
#         wallet_address=wallet,
#     )

#     if isinstance(data, dict) and data.get(THIRD_PARTY_SUCCESS_KEY) is not None:
#         # Failure path (we bubbled the whole response)
#         pprint("Failed to fetch token balances:")
#         pprint(data)
#     else:
#         # Success path returns the balances dict keyed by contract address
#         pprint("Token balances:")
#         pprint(data)
