# _etherscan_semaphore = threading.BoundedSemaphore(4)


# def get_block_by_timestamp_etherscan(unix_timestamp: int, chain: ChainData, closest: str) -> int:
#     """
#     Fetch the first block after the given UNIX timestamp
#     using Etherscan's getblocknobytime endpoint.
#     """
#     with _etherscan_semaphore:
#         params = {
#             "module": "block",
#             "action": "getblocknobytime",
#             "timestamp": str(unix_timestamp),
#             "closest": closest,
#             "chainid": str(chain.chain_id),
#             "apikey": os.getenv("ETHERSCAN_API_KEY"),
#         }
#         for i in range(4):
#             try:
#                 resp = requests.get("https://api.etherscan.io/v2/api", params=params)
#                 result = resp.json()["result"]
#                 block = int(result)

#                 # we get this error invalid literal for int() with base 10: 'Error! No closest block found'
#                 # for a time 17 minutes ago on base
#                 # maybe etherscan is not reliable here

#                 return block
#             except ValueError as e:
#                 if i < 3:
#                     time.sleep(1 + (2**i))

# deprecated, using defi llama instead
