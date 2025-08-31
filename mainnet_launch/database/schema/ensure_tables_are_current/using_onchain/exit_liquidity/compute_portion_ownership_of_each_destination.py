# """
# Get a live view of the % ownership of each pool, owned accross all autopools

# """

# import pandas as pd

# from mainnet_launch.constants import ChainData


# from mainnet_launch.data_fetching.quotes.get_all_underlying_reserves import (
#     fetch_raw_amounts_by_destination,
#     get_pools_underlying_and_total_supply,
# )


# def get_portion_ownership_by_pool(block: int, chain: ChainData) -> pd.DataFrame:

#     df = fetch_raw_amounts_by_destination(
#         block=block,
#         chain=chain,
#     )

#     states = get_pools_underlying_and_total_supply(
#         destination_vaults=df["vault_address"].unique(),
#         block=block,
#         chain=chain,
#     )

#     records = {}
#     for (vault_address, key), value in states.items():
#         if vault_address not in records:
#             records[vault_address] = {}
#         records[vault_address][key] = str(value)

#     portion_ownership_by_destination_df = pd.DataFrame.from_dict(records, orient="index").reset_index()

#     portion_ownership_by_destination_df["portion_ownership"] = portion_ownership_by_destination_df.apply(
#         lambda row: int(row["totalSupply"]) / int(row["underlyingTotalSupply"]), axis=1
#     )

#     return portion_ownership_by_destination_df
