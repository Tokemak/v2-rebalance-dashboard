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
from mainnet_launch.constants import AutopoolConstants, CACHE_TIME, AUTO_LRT
from mainnet_launch.abis.abis import (
    AUTOPOOL_ETH_STRATEGY_ABI,
    ERC_20_ABI,
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
        df = fetch_events(contract.events.UnderlyingDeposited)
        df["contract_address"] = contract.address
        dfs.append(df)

    UnderlyingDeposited_df = pd.concat(dfs, axis=0)
    return UnderlyingDeposited_df


def _fetch_destination_UnderlyingWithdraw(autopool: AutopoolConstants) -> pd.DataFrame:

    destinations = [d for d in get_destination_details() if d.autopool == autopool]
    vaultAddresses = list(set([d.vaultAddress for d in destinations]))
    dfs = []

    for vault_address in vaultAddresses:
        contract = eth_client.eth.contract(
            eth_client.toChecksumAddress(vault_address), abi=BALANCER_AURA_DESTINATION_VAULT_ABI
        )
        df = fetch_events(contract.events.UnderlyingWithdraw)
        df["contract_address"] = contract.address
        dfs.append(df)

    UnderlyingWithdraw_df = pd.concat(dfs, axis=0)
    return UnderlyingWithdraw_df


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
        1.0  # movements to or from the autopool itself are always in WETH
    )
    validated_spot_price_df["block"] = validated_spot_price_df["block"].astype(int)
    validated_spot_price_df = validated_spot_price_df.reset_index(drop=True)
    return validated_spot_price_df


def _fetch_weth_transfers_to_or_from_autopool_vault(autopool: AutopoolConstants) -> pd.DataFrame:
    start_block = 20722908

    WETH = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"
    weth_contract = eth_client.eth.contract(WETH, abi=ERC_20_ABI)

    weth_to_autopool = fetch_events(
        weth_contract.events.Transfer,
        start_block=start_block,
        argument_filters={"to": autopool.autopool_eth_addr},
    )

    weth_from_autopool = fetch_events(
        weth_contract.events.Transfer,
        start_block=start_block,
        argument_filters={"from": autopool.autopool_eth_addr},
    )

    return weth_to_autopool, weth_from_autopool


def _fetch_events_needed_to_compute_rebalance_costs(autopool: AutopoolConstants) -> pd.DataFrame:
    """
    Returns a df of event, block, hash, outDestination Vault, inDestinationVault for rebalance between destinations
    or to/from idle
    """
    strategy_contract = eth_client.eth.contract(autopool.autopool_eth_strategy_addr, abi=AUTOPOOL_ETH_STRATEGY_ABI)

    rebalance_between_destinations_df = fetch_events(strategy_contract.events.RebalanceBetweenDestinations)

    rebalance_between_destinations_df["outDestinationVault"] = rebalance_between_destinations_df[
        "outSummaryStats"
    ].apply(lambda x: eth_client.toChecksumAddress(x[0]))

    rebalance_between_destinations_df["inDestinationVault"] = rebalance_between_destinations_df["inSummaryStats"].apply(
        lambda x: eth_client.toChecksumAddress(x[0])
    )

    rebalance_to_idle_df = fetch_events(strategy_contract.events.RebalanceToIdle)

    rebalance_to_idle_df["outDestinationVault"] = rebalance_to_idle_df["outSummary"].apply(
        lambda x: eth_client.toChecksumAddress(x[0])
    )

    rebalance_to_idle_df["inDestinationVault"] = autopool.autopool_eth_addr

    # rebalance_columns = ["event", "block", "hash", "outDestinationVault", "inDestinationVault"]
    # rebalance_to_idle_df = rebalance_to_idle_df[rebalance_columns].copy()
    # rebalance_between_destinations_df = rebalance_between_destinations_df[rebalance_columns].copy()

    rebalance_df = pd.concat([rebalance_to_idle_df, rebalance_between_destinations_df], axis=0)

    weth_to_autopool, weth_from_autopool = _fetch_weth_transfers_to_or_from_autopool_vault(autopool)
    UnderlyingDeposited_df = _fetch_destination_UnderlyingDeposited(autopool)
    UnderlyingWithdraw_df = _fetch_destination_UnderlyingWithdraw(autopool)

    valid_weth_from_autopool = weth_from_autopool[~weth_from_autopool["hash"].duplicated(keep=False)].copy()
    valid_weth_from_autopool["weth_from_autopool"] = valid_weth_from_autopool["value"] / 1e18

    valid_weth_to_autopool = weth_to_autopool[~weth_to_autopool["hash"].duplicated(keep=False)].copy()
    valid_weth_to_autopool["weth_to_autopool"] = valid_weth_to_autopool["value"] / 1e18

    valid_underlying_withdraw_df = UnderlyingWithdraw_df[~UnderlyingWithdraw_df["hash"].duplicated(keep=False)].copy()
    valid_underlying_withdraw_df["amount_withdrawn"] = valid_underlying_withdraw_df["amount"] / 1e18

    valid_underlying_deposited_df = UnderlyingDeposited_df[
        ~UnderlyingDeposited_df["hash"].duplicated(keep=False)
    ].copy()
    valid_underlying_deposited_df["amount_deposited"] = valid_underlying_deposited_df["amount"] / 1e18

    # amount_deposited the quanity of LP tokens deposited to a destination (value out the autopool)
    # amount_withdrawn the quanity of LP tokens withdrawn from a destination (value into the autopool)
    rebalance_df = pd.merge(
        rebalance_df, valid_underlying_withdraw_df[["amount_withdrawn", "hash"]], on="hash", how="left"
    )
    rebalance_df = pd.merge(
        rebalance_df, valid_underlying_deposited_df[["amount_deposited", "hash"]], on="hash", how="left"
    )
    rebalance_df = pd.merge(rebalance_df, valid_weth_to_autopool[["weth_to_autopool", "hash"]], on="hash", how="left")
    rebalance_df = pd.merge(
        rebalance_df, valid_weth_from_autopool[["weth_from_autopool", "hash"]], on="hash", how="left"
    )

    return rebalance_df


@st.cache_data(ttl=CACHE_TIME)
def fetch_rebalance_events_actual_amounts(autopool: AutopoolConstants) -> pd.DataFrame:
    rebalance_df = _fetch_events_needed_to_compute_rebalance_costs(autopool)
    # get the price of each destination token before the rebalance
    validated_spot_price_df = _fetch_lp_token_validated_spot_price(rebalance_df["block"] - 1, autopool)
    validated_spot_price_df["block"] = validated_spot_price_df["block"] + 1  # set the block to be the blocks +1
    rebalance_df = pd.merge(rebalance_df, validated_spot_price_df, on="block", how="left")
    rebalance_df["amount_deposited"] = rebalance_df["amount_deposited"].combine_first(rebalance_df["weth_to_autopool"])
    rebalance_df["amount_withdrawn"] = rebalance_df["amount_withdrawn"].combine_first(
        rebalance_df["weth_from_autopool"]
    )

    def _compute_value_out_of_autopool(row):
        out_price = row[row["outDestinationVault"]]
        out_amount = row["amount_withdrawn"]
        in_price = row[row["inDestinationVault"]]
        in_amount = row["amount_deposited"]

        return out_price, out_amount, in_price, in_amount

    rebalance_df[["out_price", "out_amount", "in_price", "in_amount"]] = rebalance_df.apply(
        lambda row: _compute_value_out_of_autopool(row), axis=1, result_type="expand"
    )

    rebalance_df["spot_value_out"] = rebalance_df["out_price"] * rebalance_df["out_amount"]
    rebalance_df["spot_value_in"] = rebalance_df["in_price"] * rebalance_df["in_amount"]
    rebalance_df["swap_cost"] = rebalance_df["spot_value_out"] - rebalance_df["spot_value_in"]

    # Donations to the autopool make the swap cost negative, this throws off a lot of math so
    # the same is true because the solver can have some excess in rebalance before sendign it back into the pool
    # on a later rebalance
    # a negative swap cost throws off some of the stats, so treat it as 0
    rebalance_df["swap_cost"] = rebalance_df["swap_cost"].clip(lower=0)
    rebalance_df = add_timestamp_to_df_with_block_column(rebalance_df)
    return rebalance_df
