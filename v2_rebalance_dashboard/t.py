# # from web3 import Web3
# # from multicall import Call, Multicall

# # class OffchainCalculator:
# #     def __init__(self, eth_client, pricer_address, pool_address, dex_stats_address):
# #         self.eth_client = eth_client
# #         self.pricer_address = pricer_address
# #         self.pool_address = pool_address
# #         self.dex_stats_address = dex_stats_address

# #     def calculate_reserve_in_eth_by_index(self, index):
# #         """
# #         Offchain calculation of reserve in ETH by index, mimicking the Solidity internal function.
        
# #         :param pricer: Address of the IRootPriceOracle contract.
# #         :param index: The index of the token in the reserveTokens array.
# #         :return: The calculated reserve in ETH.
# #         """

# #         # Setup Multicall
# #         multicall = Multicall(
# #             [
# #                 # Get the token address by index
# #                 Call(self.dex_stats_address, ['reserveTokens(uint256)(address)', index], [['token_address', None]]),

# #                 # Get the token decimals
# #                 Call(lambda values: values['token_address'], ['decimals()(uint8)'], [['decimals', None]]),

# #                 # Get the price of the token in ETH from the pricer contract
# #                 Call(self.pricer_address, ['getPriceInEth(address)(uint256)', lambda values: values['token_address']], [['price_in_eth', None]]),

# #                 # Get the token balance from the pool
# #                 Call(self.pool_address, ['balances(uint256)(uint256)', index], [['balance', None]])
# #             ],
# #             _w3=self.eth_client
# #         )

# #         # Execute the multicall
# #         result = multicall()

# #         # Extract results

# #         price_in_eth = result['price_in_eth']
# #         balance = result['balance']

# #         # Perform the off-chain calculation
# #         decimals = result['decimals']
# #         divisor = 10 ** decimals
# #         calculated_reserve_in_eth = (price_in_eth * balance) / divisor

# #         # Return the result in ETH (adjusted for 18 decimals)
# #         return calculated_reserve_in_eth / 1e18

# # # Example usage:
# # from v2_rebalance_dashboard.get_state_by_block import eth_client
# # pricer_address = '0x28B7773089C56Ca506d4051F0Bc66D247c6bdb3a'
# # dex_stats_address = '0xCc4d593D3EdE5EF5b70D28Ac5c5627F1DD0523E8'
# # pool_address = '0x58AAdFB1Afac0ad7fca1148f3cdE6aEDF5236B6D '
# # calculator = OffchainCalculator(eth_client, pricer_address=pricer_address, pool_address=pool_address, dex_stats_address=dex_stats_address)
# # a = calculator.calculate_reserve_in_eth_by_index(0)
# # print(a)

# from web3 import Web3
# from multicall import Call, Multicall

# class OffchainCalculator:
#     def __init__(self, eth_client, pricer_address, pool_address, dex_stats_address):
#         self.eth_client = eth_client
#         self.pricer_address = pricer_address
#         self.pool_address = pool_address
#         self.dex_stats_address = dex_stats_address

#     def calculate_reserve_in_eth_by_index(self, index):
#         """
#         Offchain calculation of reserve in ETH by index, mimicking the Solidity internal function.
        
#         :param index: The index of the token in the reserveTokens array.
#         :return: The calculated reserve in ETH.
#         """

#         # Stage 1: Get the token address by index
#         token_call = Multicall(
#             [
#                 Call(self.dex_stats_address, ['reserveTokens(uint256)(address)', index], [['token_address', None]])
#             ],
#             _w3=self.eth_client
#         )
        
#         # Execute the first multicall
#         token_result = token_call()

#         # Extract the token address
#         token_address = token_result['token_address']

#         # Stage 2: Perform the remaining multicalls using the token address
#         data_call = Multicall(
#             [
#                 # Get the token decimals
#                 Call(token_address, ['decimals()(uint8)'], [['decimals', None]]),

#                 # Get the price of the token in ETH from the pricer contract
#                 Call(self.pricer_address, ['getPriceInEth(address)(uint256)', token_address], [['price_in_eth', None]]),

#                 # Get the token balance from the pool
#                 Call(self.pool_address, ['balances(uint256)(uint256)', index], [['balance', None]])
#             ],
#             _w3=self.eth_client
#         )

#         # Execute the second multicall
#         result = data_call()

#         # Extract results
#         decimals = result['decimals']
#         price_in_eth = result['price_in_eth']
#         balance = result['balance']

#         # Perform the off-chain calculation
#         divisor = 10 ** decimals
#         calculated_reserve_in_eth = (price_in_eth * balance) / divisor

#         # Return the result in ETH (adjusted for 18 decimals)
#         return calculated_reserve_in_eth / 1e18

# # Example usage:
# from v2_rebalance_dashboard.get_state_by_block import eth_client
# pricer_address = '0x28B7773089C56Ca506d4051F0Bc66D247c6bdb3a'
# dex_stats_address = '0x4597b57ec8f147EEB5738bCC2236AE8269759dF8'
# pool_address = '0x60329c3C21E8FE14901eca479F41A6D6940843a7'
# calculator = OffchainCalculator(eth_client, pricer_address=pricer_address, pool_address=pool_address, dex_stats_address=dex_stats_address)
# a = calculator.calculate_reserve_in_eth_by_index(0)
# print(a)
# # this is very close, can come back to later, only works on curve