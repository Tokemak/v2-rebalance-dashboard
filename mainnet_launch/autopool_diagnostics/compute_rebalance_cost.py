import pandas as pd
import streamlit as st
from multicall import Call


from mainnet_launch.data_fetching.get_state_by_block import (
    get_raw_state_by_blocks,
    safe_normalize_with_bool_success,
    eth_client,
)

from mainnet_launch.data_fetching.add_info_to_dataframes import add_timestamp_to_df_with_block_column
from mainnet_launch.data_fetching.get_events import fetch_events
from mainnet_launch.constants import AutopoolConstants, CACHE_TIME
from mainnet_launch.abis.abis import (
    AUTOPOOL_ETH_STRATEGY_ABI,
    BALANCER_AURA_DESTINATION_VAULT_ABI,
)

from mainnet_launch.destinations import get_destination_details


def _fetch_destination_UnderlyingDeposited(autopool: AutopoolConstants) -> pd.DataFrame:

    destinations = [d for d in get_destination_details() if d.autopool == autopool]
    vaultAddresses = list(set([d.vaultAddress for d in destinations]))
    dfs = []

    for vault_address in vaultAddresses:
        contract = eth_client.eth.contract(
            eth_client.toChecksumAddress(vault_address), abi=BALANCER_AURA_DESTINATION_VAULT_ABI
        )
        df = fetch_events(contract.events.UnderlyingDeposited, start_block=20538409)
        df["contract_address"] = contract.address
        dfs.append(df)

    UnderlyingDeposited_df = pd.concat(dfs, axis=0)
    return UnderlyingDeposited_df


def _fetch_lp_token_validated_spot_price(blocks: list[int], autopool: AutopoolConstants) -> pd.DataFrame:

    destinations = [d for d in get_destination_details() if d.autopool == autopool]

    get_validated_spot_price_calls = []
    for dest in destinations:
        call = Call(
            dest.vaultAddress,
            ["getValidatedSpotPrice()(uint256)"],
            [(dest.vaultAddress, safe_normalize_with_bool_success)],
        )
        get_validated_spot_price_calls.append(call)

    validated_spot_price_df = get_raw_state_by_blocks(get_validated_spot_price_calls, blocks, include_block_number=True)
    validated_spot_price_df[autopool.autopool_eth_addr] = (
        1.0 # movements to or from the autopool itself are always in WETH
    )
    validated_spot_price_df["block"] = validated_spot_price_df["block"].astype(int)
    return validated_spot_price_df


def _fetch_all_rebalance_events(autopool: AutopoolConstants) -> pd.DataFrame:
    strategy_contract = eth_client.eth.contract(autopool.autopool_eth_strategy_addr, abi=AUTOPOOL_ETH_STRATEGY_ABI)

    rebalance_between_destinations_df = fetch_events(strategy_contract.events.RebalanceBetweenDestinations)
    rebalance_between_destinations_df["outDestinationVault"] = rebalance_between_destinations_df[
        "outSummaryStats"
    ].apply(lambda x: eth_client.toChecksumAddress(x[0]))

    rebalance_between_destinations_df["inDestinationVault"] = rebalance_between_destinations_df["inSummaryStats"].apply(
        lambda x: eth_client.toChecksumAddress(x[0])
    )

    rebalance_between_destinations_df["tokenIn"] = rebalance_between_destinations_df["params"].apply(
        lambda x: eth_client.toChecksumAddress(x[1])
    )

    rebalance_between_destinations_df["tokenOut"] = rebalance_between_destinations_df["params"].apply(
        lambda x: eth_client.toChecksumAddress(x[4])
    )

    rebalance_between_destinations_df["amountOut"] = rebalance_between_destinations_df["params"].apply(
        lambda x: x[5]
        / 1e18  # the amout of lp tokens taken out is always correct, what we don't know is the amount of lp tokens going in
    )

    rebalance_between_destinations_df["swapCost"] = rebalance_between_destinations_df["valueStats"].apply(
        lambda x: x[4] / 1e18
    )

    return rebalance_between_destinations_df


@st.cache_data(ttl=CACHE_TIME)
def fetch_spot_value_swap_cost_df(autopool: AutopoolConstants) -> pd.DataFrame:
    """

    Returns the spot value of the swap cost by compaaring the spot value of the LP tokens that are added or
    withdrawn from the pool

    idle -> dest
    and
    dest -> idle

    ignores

    dest -> idle (infrequent and small) can add later
    and
    dest -> dest when the lp token is the same. Those have a swap cost of 0

    """
    rebalance_between_destinations_df = _fetch_all_rebalance_events(autopool)
    blocks = rebalance_between_destinations_df["block"] 
    validated_spot_price_df = _fetch_lp_token_validated_spot_price(blocks  -1, autopool)
    validated_spot_price_df['block'] = blocks.values
    UnderlyingDeposited_df = _fetch_destination_UnderlyingDeposited(autopool)

    # only look at the deposit transactions that are alone in the hash.
    # those transaction with more than one deposit into an underlying destination
    # are moving same same LP tokens between destinations those have a swap cost of 0
    # so they can be safely ignored

    valid_deposit_hashes = (
        UnderlyingDeposited_df["hash"].value_counts()[UnderlyingDeposited_df["hash"].value_counts() == 1].index
    )
    valid_underlying_deposited_df = UnderlyingDeposited_df[
        UnderlyingDeposited_df["hash"].isin(valid_deposit_hashes)
    ].copy()
    valid_underlying_deposited_df["amountIn"] = valid_underlying_deposited_df["amount"] / 1e18
    # this is the correct amount of LP tokens minted during this rebalance transaction

    df = pd.merge(rebalance_between_destinations_df, valid_underlying_deposited_df, on="hash", how="inner")
    df["raw_amount_out"] = df["params"].apply(lambda x: x[5])
    df["raw_amount_in"] = df["amount"]
    df["event_amount_in"] = df["params"].apply(lambda x: x[2])
    df["block"] = df["block_y"]

    limited_df = df[
        [
            "contract_address",
            "amountIn",
            "amountOut",  # amount out is the quantity of LP tokens or WETH removed from the vault
            "inDestinationVault",
            "outDestinationVault",
            "hash",
            "block",
            "raw_amount_in",
            "raw_amount_out",
            "event_amount_in",
            "swapCost",
        ]
    ].copy()

    amounts_with_spot_values = pd.merge(limited_df, validated_spot_price_df, on="block", how="inner")

    def _compute_spot_value_change(row):
        in_dest = row["inDestinationVault"]
        in_dest_spot_price = row[in_dest]

        out_dest = row["outDestinationVault"]
        out_dest_spot_price = row[out_dest]

        spot_value_out = out_dest_spot_price * row["amountOut"]
        spot_value_in = in_dest_spot_price * row["amountIn"]

        swap_cost = spot_value_out - spot_value_in

        return {"spot_value_out": spot_value_out, "spot_value_in": spot_value_in, "spot_value_swap_cost": swap_cost}

    spot_values_df = pd.DataFrame.from_records(
        amounts_with_spot_values.apply(_compute_spot_value_change, axis=1).values
    )
    amounts_with_spot_values = pd.concat([amounts_with_spot_values, spot_values_df], axis=1)

    # if the solver donates more to the pool than it takes out, treat that as a swap cost of 0 instead of a negative swap cost
    amounts_with_spot_values["spot_value_swap_cost"] = amounts_with_spot_values["spot_value_swap_cost"].clip(lower=0)
    amounts_with_spot_values = add_timestamp_to_df_with_block_column(amounts_with_spot_values)
    # outDestinationVault == autopool vault address means idle -> dest
    # else
    # dest to dest
    return amounts_with_spot_values[["spot_value_swap_cost", "inDestinationVault", "outDestinationVault", "swapCost", "block"]]
