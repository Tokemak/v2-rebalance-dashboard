import streamlit as st
import pandas as pd


from mainnet_launch.constants import AutopoolConstants, CACHE_TIME, eth_client
from mainnet_launch.lens_contract import get_pools_and_destinations_call
from mainnet_launch.data_fetching.get_state_by_block import get_raw_state_by_blocks
from mainnet_launch.destinations import DestinationDetails, get_destination_details


@st.cache_data(ttl=CACHE_TIME)
def fetch_destination_totalEthValueHeldIfNoDiscount(autopool: AutopoolConstants, blocks) -> pd.Series:
    """
    Returns a dataframe with one column `nav_per_share_if_no_discount` for each block in blocks.
    """
    details = [d for d in get_destination_details() if d.autopool == autopool]
    # autopoolNav in Idle df
    lp_token_addresss_to_name = {d.lpTokenAddress: d.vault_name for d in details if d.lpTokenAddress is not None}

    pool_and_destinations_df = get_raw_state_by_blocks([get_pools_and_destinations_call()], blocks)

    def _extract_totalEthValueHeldIfNoDiscount(row: dict):

        for a, destination_list in zip(row["autopools"], row["destinations"]):
            if a["poolAddress"].lower() == autopool.autopool_eth_addr.lower():
                # overestimate because we hold more of the discounted asset than if the price was at peg
                # The invarients have the LP always holding more of the less valueable asset

                totalEthValueHeldIfNoDiscount = {
                    autopool.name: a["totalIdle"] / 1e18
                }  # ETH in idle is not at any disocunt

                for dest in destination_list:
                    discounts = [lst_stats["discount"] / 1e18 for lst_stats in dest["lstStatsData"]]
                    valueHeldInEth = [t["valueHeldInEth"] / 1e18 for t in dest["underlyingTokenValueHeld"]]
                    # a positive discount means we think the destination is trading at below its backing on the consensus layer
                    # a negative disocunt means we think the destination is trading at more than the backing on the consensus layer
                    valueHeldInEth_removed_lst_discount = float(
                        sum([value / (1 - discount) for discount, value, in zip(discounts, valueHeldInEth)])
                    )
                    destination_name = lp_token_addresss_to_name[eth_client.toChecksumAddress(dest["lpTokenAddress"])]

                    if destination_name not in totalEthValueHeldIfNoDiscount:
                        totalEthValueHeldIfNoDiscount[destination_name] = valueHeldInEth_removed_lst_discount
                    else:
                        totalEthValueHeldIfNoDiscount[destination_name] += valueHeldInEth_removed_lst_discount

        return totalEthValueHeldIfNoDiscount

    def _extract_total_shares(row: dict):
        for a, _ in zip(row["autopools"], row["destinations"]):
            if a["poolAddress"].lower() == autopool.autopool_eth_addr.lower():
                return a["totalSupply"] / 1e18

    total_shares = pool_and_destinations_df["getPoolsAndDestinations"].apply(_extract_total_shares)

    eth_value_if_no_discount_df = pd.DataFrame.from_records(
        pool_and_destinations_df["getPoolsAndDestinations"].apply(_extract_totalEthValueHeldIfNoDiscount),
        index=pool_and_destinations_df.index,
    )

    eth_value_if_no_discount_df["nav_if_all_lp_tokens_return_to_peg"] = eth_value_if_no_discount_df.sum(axis=1)
    eth_value_if_no_discount_df["total_shares"] = total_shares
    eth_value_if_no_discount_df["nav_per_share_if_no_discount"] = (
        eth_value_if_no_discount_df["nav_if_all_lp_tokens_return_to_peg"] / eth_value_if_no_discount_df["total_shares"]
    )

    return eth_value_if_no_discount_df
