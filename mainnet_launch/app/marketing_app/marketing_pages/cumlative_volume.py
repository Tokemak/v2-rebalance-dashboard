# """Hit the subgraph to get the all time cumulative USD volume moved in automated rebalances"""


# def get_mainnet_usd_volumme


# query getAutopoolRebalances($address: String!) {
#   autopoolRebalances(
#     where: {autopool: $address}
#     orderBy: timestamp
#     orderDirection: desc
#     first: 1
#   ) {
#     autopool
#     timestamp
#     blockNumber

#     tokenOutValueInEth
#     tokenOutValueBaseAsset

#   }
# }


# on mianet
