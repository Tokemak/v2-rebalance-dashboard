import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
import streamlit as st
from multicall import Call
from web3 import Web3
import concurrent.futures


from mainnet_launch.constants.constants import (
    AutopoolConstants,
    WETH,
    ROOT_PRICE_ORACLE,
    ChainData,
    ALL_AUTOPOOLS,
)
from mainnet_launch.app.app_config import NUM_GAS_INFO_FETCHING_THREADS

from mainnet_launch.data_fetching.get_state_by_block import (
    get_raw_state_by_blocks,
    safe_normalize_with_bool_success,
    get_state_by_one_block,
    identity_with_bool_success,
)

from mainnet_launch.data_fetching.get_events import fetch_events, FetchEventParams, fetch_many_events

from mainnet_launch.abis import (
    AUTOPOOL_ETH_STRATEGY_ABI,
    ERC_20_ABI,
    BALANCER_AURA_DESTINATION_VAULT_ABI,
    ROOT_PRICE_ORACLE_ABI,
)
from mainnet_launch.destinations import get_destination_details
from mainnet_launch.data_fetching.add_info_to_dataframes import (
    add_timestamp_to_df_with_block_column,
    add_transaction_gas_info_to_df_with_tx_hash,
)
from mainnet_launch.database.database_operations import (
    write_dataframe_to_table,
    run_read_only_query,
    get_earliest_block_from_table_with_autopool,
    get_all_rows_in_table_by_autopool,
)
from mainnet_launch.database.should_update_database import should_update_table


REBALANCE_EVENTS_TABLE = "REBALANCE_EVENTS_TABLE"


def fetch_and_render_rebalance_events_data(autopool: AutopoolConstants):
    rebalance_df = fetch_rebalance_events_df(autopool)
    rebalance_figures = _make_rebalance_events_plots(rebalance_df)
    st.header(f"{autopool.symbol} Rebalance Events")

    for figure in rebalance_figures:
        st.plotly_chart(figure, use_container_width=True)


def add_new_rebalance_events_for_each_autopool_to_table(run_anyway: bool = False):
    if should_update_table(REBALANCE_EVENTS_TABLE) or run_anyway:
        for autopool in ALL_AUTOPOOLS:
            # if this process gets interrupted, then you could have data for some autopools but not all of them
            highest_block_already_fetched = get_earliest_block_from_table_with_autopool(
                REBALANCE_EVENTS_TABLE, autopool
            )
            new_rebalance_events_df = fetch_rebalance_events_df_from_external_source(
                autopool, highest_block_already_fetched
            )
            write_dataframe_to_table(new_rebalance_events_df, REBALANCE_EVENTS_TABLE)

    # make sure that all teh autopool shave some data
    for autopool in ALL_AUTOPOOLS:
        rebalance_events_df = get_all_rows_in_table_by_autopool(REBALANCE_EVENTS_TABLE, autopool)
        if len(rebalance_events_df) == 0:
            # if this process gets interrupted, then you could have data for some autopools but not all of them
            highest_block_already_fetched = get_earliest_block_from_table_with_autopool(
                REBALANCE_EVENTS_TABLE, autopool
            )
            new_rebalance_events_df = fetch_rebalance_events_df_from_external_source(
                autopool, highest_block_already_fetched
            )
            write_dataframe_to_table(new_rebalance_events_df, REBALANCE_EVENTS_TABLE)


def fetch_rebalance_events_df(autopool: AutopoolConstants) -> pd.DataFrame:
    add_new_rebalance_events_for_each_autopool_to_table()
    rebalance_events_df = get_all_rows_in_table_by_autopool(REBALANCE_EVENTS_TABLE, autopool)
    rebalance_events_df["actual_swap_cost"] = rebalance_events_df["outEthValue"] - rebalance_events_df["inEthValue"]
    return rebalance_events_df


def fetch_rebalance_events_df_from_external_source(autopool: AutopoolConstants, start_block: int) -> pd.DataFrame:
    rebalance_df = fetch_rebalance_events_and_actual_weth_and_lp_tokens_moved(autopool, start_block)
    destination_details = get_destination_details(autopool)
    destination_vault_address_to_symbol = {dest.vaultAddress: dest.vault_name for dest in destination_details}

    def _make_rebalance_between_destination_human_readable(
        row: dict,
    ) -> dict:

        swapCost = float(row["swap_cost"])

        inEthValue = row["spot_value_in"]
        outEthValue = row["spot_value_out"]
        slippage = swapCost / outEthValue

        in_destination_symbol = destination_vault_address_to_symbol[row["inDestinationVault"]]
        out_destination_symbol = destination_vault_address_to_symbol[row["outDestinationVault"]]
        moveName = f"{out_destination_symbol} -> {in_destination_symbol}"

        if row["event"] == "RebalanceBetweenDestinations":
            predictedAnnualizedGain = float(row["predictedAnnualizedGain"]) / 1e18
            predicted_gain_during_swap_cost_off_set_period = predictedAnnualizedGain * (row["swapOffsetPeriod"] / 365)
            out_compositeReturn = 100 * float(row["outSummaryStats"][9]) / 1e18
            in_compositeReturn = 100 * float(row["inSummaryStats"][9]) / 1e18
            apr_delta = in_compositeReturn - out_compositeReturn
            predicted_increase_after_swap_cost = predicted_gain_during_swap_cost_off_set_period - swapCost
            break_even_days = swapCost / (predictedAnnualizedGain / 365)
            offset_period = row["swapOffsetPeriod"]

            return {
                "break_even_days": break_even_days,
                "swapCost": swapCost,
                "swapCostIdle": 0,
                "swapCostChurn": swapCost,
                "apr_delta": apr_delta,
                "out_compositeReturn": out_compositeReturn,
                "in_compositeReturn": in_compositeReturn,
                "predicted_increase_after_swap_cost": predicted_increase_after_swap_cost,
                "predicted_gain_during_swap_cost_off_set_period": predicted_gain_during_swap_cost_off_set_period,
                "inEthValue": inEthValue,
                "outEthValue": outEthValue,
                "out_destination": row["outDestinationVault"],
                "in_destination": row["inDestinationVault"],
                "offset_period": offset_period,
                "slippage": slippage,
                "hash": row["hash"],
                "moveName": moveName,
            }

        elif row["event"] == "RebalanceToIdle":

            out_compositeReturn = 100 * float(row["outSummary"][9]) / 1e18
            in_compositeReturn = 0
            apr_delta = in_compositeReturn - out_compositeReturn
            return {
                "break_even_days": None,
                "swapCost": swapCost,
                "swapCostIdle": swapCost,
                "swapCostChurn": 0,
                "apr_delta": apr_delta,
                "out_compositeReturn": out_compositeReturn,
                "in_compositeReturn": in_compositeReturn,
                "predicted_increase_after_swap_cost": None,
                "predicted_gain_during_swap_cost_off_set_period": None,
                "inEthValue": inEthValue,
                "outEthValue": outEthValue,
                "out_destination": row["outDestinationVault"],
                "in_destination": row["inDestinationVault"],
                "offset_period": None,
                "slippage": slippage,
                "hash": row["hash"],
                "moveName": moveName,
            }
        else:
            raise ValueError("Unexpected event name", row["event"])

    clean_rebalance_df = pd.DataFrame.from_records(
        rebalance_df.apply(lambda row: _make_rebalance_between_destination_human_readable(row), axis=1)
    )
    clean_rebalance_df = pd.merge(rebalance_df, clean_rebalance_df, on="hash")

    clean_rebalance_df = add_transaction_gas_info_to_df_with_tx_hash(clean_rebalance_df, autopool.chain)

    def _get_flash_borrower_address(tx_hash: str, chain: ChainData) -> str:
        # get the address of the flash borrower that did this rebalance
        return chain.client.eth.get_transaction(tx_hash)["to"]

    with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_GAS_INFO_FETCHING_THREADS) as executor:
        flash_borrower_addresses = list(
            executor.map(
                lambda tx_hash: _get_flash_borrower_address(tx_hash, autopool.chain), clean_rebalance_df["hash"]
            )
        )

    clean_rebalance_df["flash_borrower_address"] = flash_borrower_addresses

    clean_rebalance_df = _add_solver_profit_cols(clean_rebalance_df, autopool)

    clean_rebalance_df = add_timestamp_to_df_with_block_column(clean_rebalance_df, autopool.chain)
    clean_rebalance_df["timestamp"] = clean_rebalance_df.index

    clean_rebalance_df["autopool"] = autopool.symbol
    # these columns are currently being used at least one plot the dashboard
    used_cols = [
        "event",
        "hash",
        "block",
        "outEthValue",
        "inEthValue",
        "moveName",
        "gasCostInETH",
        "out_compositeReturn",
        "in_compositeReturn",
        "predicted_gain_during_swap_cost_off_set_period",
        "swapCost",
        "slippage",
        "break_even_days",
        "offset_period",
        "solver_profit",
        "predicted_increase_after_swap_cost",
        "outDestinationVault",
        "inDestinationVault",
        "autopool",
        "gas_price",
        "gas_used",
        "timestamp",
    ]

    return clean_rebalance_df[used_cols]


def getPriceInEth_call(name: str, token_address: str, chain: ChainData) -> Call:
    return Call(
        ROOT_PRICE_ORACLE(chain),
        ["getPriceInEth(address)(uint256)", token_address],
        [(name, safe_normalize_with_bool_success)],
    )


def _add_solver_profit_cols(clean_rebalance_df: pd.DataFrame, autopool: AutopoolConstants):
    all_flash_borrowers = clean_rebalance_df["flash_borrower_address"].unique()
    rebalance_dfs = []
    for flash_borrower_address in all_flash_borrowers:
        limited_clean_rebalance_df = clean_rebalance_df[
            clean_rebalance_df["flash_borrower_address"] == flash_borrower_address
        ].copy()
        limited_clean_rebalance_df = _add_solver_profit_cols_by_flash_borrower(
            limited_clean_rebalance_df, flash_borrower_address, autopool.chain
        )
        rebalance_dfs.append(limited_clean_rebalance_df)

    all_clean_rebalance_df = pd.concat(rebalance_dfs)
    return all_clean_rebalance_df


def _add_solver_profit_cols_by_flash_borrower(
    limited_clean_rebalance_df: pd.DataFrame, flash_borrower_address: str, chain: ChainData
) -> list[Call]:
    """
    Solver profit: ETH value held by the solver AFTER a rebalance - ETH value held by the solver BEFORE a rebalance
    """
    root_price_oracle_contract = chain.client.eth.contract(ROOT_PRICE_ORACLE(chain), abi=ROOT_PRICE_ORACLE_ABI)
    tokens: list[str] = fetch_events(root_price_oracle_contract.events.TokenRegistered, chain=chain)["token"].values

    symbol_calls = [Call(t, ["symbol()(string)"], [(t, identity_with_bool_success)]) for t in tokens]
    block = int(limited_clean_rebalance_df["block"].max())
    token_address_to_symbol = get_state_by_one_block(symbol_calls, block, chain)

    price_calls = [getPriceInEth_call(token_address_to_symbol[t], t, chain) for t in tokens]
    balance_of_calls = [
        Call(
            t,
            ["balanceOf(address)(uint256)", flash_borrower_address],
            [(token_address_to_symbol[t], safe_normalize_with_bool_success)],
        )
        for t in tokens
    ]

    # compare the ETH value in the solver before and after a rebalance
    value_before_df = _build_value_held_by_solver(
        balance_of_calls, price_calls, limited_clean_rebalance_df["block"] - 1, chain
    )
    value_after_df = _build_value_held_by_solver(
        balance_of_calls, price_calls, limited_clean_rebalance_df["block"], chain
    )

    limited_clean_rebalance_df["before_rebalance_eth_value_of_solver"] = value_before_df[
        "total_eth_value"
    ].values.astype(float)

    limited_clean_rebalance_df["after_rebalance_eth_value_of_solver"] = value_after_df["total_eth_value"].values.astype(
        float
    )
    limited_clean_rebalance_df["solver_profit"] = (
        limited_clean_rebalance_df["after_rebalance_eth_value_of_solver"]
        - limited_clean_rebalance_df["before_rebalance_eth_value_of_solver"]
    )

    return limited_clean_rebalance_df


def _build_value_held_by_solver(
    balance_of_calls: list[Call], price_calls: list[Call], blocks: list[int], chain: ChainData
) -> pd.DataFrame:
    balance_of_df = get_raw_state_by_blocks(balance_of_calls, blocks, chain)
    price_df = get_raw_state_by_blocks(price_calls, blocks, chain)
    price_df["ETH"] = 1.0

    # make the columns are in the same order
    eth_value_held_by_flash_solver_df = price_df[balance_of_df.columns] * balance_of_df[balance_of_df.columns]
    eth_value_held_by_flash_solver_df["total_eth_value"] = eth_value_held_by_flash_solver_df.sum(axis=1)
    return eth_value_held_by_flash_solver_df


def _add_spot_value_of_rebalance_events(rebalance_df: pd.DataFrame, autopool: AutopoolConstants) -> pd.DataFrame:
    # get the price of each destination LP token before the rebalance
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
    # - Because the solver is moving leftover tokens from a previous rebalance, this is because the solver is (currently) altruistic
    # - External donations, to make up for initial deployment rebalancing costs
    # treat negative swap cost as 0

    rebalance_df["swap_cost"] = rebalance_df["swap_cost"].clip(lower=0)
    return rebalance_df


def _fetch_lp_token_validated_spot_price(blocks: list[int], autopool: AutopoolConstants) -> pd.DataFrame:

    destinations = get_destination_details(autopool)

    get_validated_spot_price_calls = []
    for dest in destinations:
        call = Call(
            dest.vaultAddress,
            ["getValidatedSpotPrice()(uint256)"],
            [(dest.vaultAddress, safe_normalize_with_bool_success)],
        )
        get_validated_spot_price_calls.append(call)

    validated_spot_price_df = get_raw_state_by_blocks(
        get_validated_spot_price_calls, blocks, chain=autopool.chain, include_block_number=True
    )
    validated_spot_price_df[autopool.autopool_eth_addr] = (
        1.0  # movements to or from the autopool itself are always in WETH
    )
    validated_spot_price_df = validated_spot_price_df.reset_index(drop=True)
    return validated_spot_price_df


def fetch_events_needed_for_rebalance_events(autopool: AutopoolConstants, start_block: int) -> list[pd.DataFrame]:
    event_dfs_to_fetch = []

    # --- WETH Transfer events ---
    weth_contract = autopool.chain.client.eth.contract(WETH(autopool.chain), abi=ERC_20_ABI)
    event_dfs_to_fetch.append(
        FetchEventParams(
            event=weth_contract.events.Transfer,
            chain=autopool.chain,
            id="weth_to_autopool",
            start_block=start_block,
            argument_filters={"to": autopool.autopool_eth_addr},
        )
    )
    event_dfs_to_fetch.append(
        FetchEventParams(
            event=weth_contract.events.Transfer,
            chain=autopool.chain,
            id="weth_from_autopool",
            start_block=start_block,
            argument_filters={"from": autopool.autopool_eth_addr},
        )
    )

    # --- Strategy events ---
    strategy_contract = autopool.chain.client.eth.contract(
        autopool.autopool_eth_strategy_addr, abi=AUTOPOOL_ETH_STRATEGY_ABI
    )
    event_dfs_to_fetch.append(
        FetchEventParams(
            event=strategy_contract.events.RebalanceBetweenDestinations,
            chain=autopool.chain,
            id="rebalance_between_destinations",
            start_block=start_block,
        )
    )
    event_dfs_to_fetch.append(
        FetchEventParams(
            event=strategy_contract.events.RebalanceToIdle,
            chain=autopool.chain,
            id="rebalance_to_idle",
            start_block=start_block,
        )
    )

    # --- Underlying Deposited / Withdraw events for each vault ---
    destinations = get_destination_details(autopool)
    vault_addresses = list({d.vaultAddress for d in destinations})

    for vault_address in vault_addresses:
        contract = autopool.chain.client.eth.contract(
            Web3.toChecksumAddress(vault_address), abi=BALANCER_AURA_DESTINATION_VAULT_ABI
        )
        # Underlying Deposited event for this vault.
        event_dfs_to_fetch.append(
            FetchEventParams(
                event=contract.events.UnderlyingDeposited,
                chain=autopool.chain,
                id=f"Underlying_deposited_{vault_address}",
                start_block=start_block,
            )
        )
        # Underlying Withdraw event for this vault.
        event_dfs_to_fetch.append(
            FetchEventParams(
                event=contract.events.UnderlyingWithdraw,
                chain=autopool.chain,
                id=f"Underlying_withdraw_{vault_address}",
                start_block=start_block,
            )
        )

    # Fetch all events concurrently.
    results = fetch_many_events(event_dfs_to_fetch, num_threads=16)
    rebalance_between_destinations_df = results["rebalance_between_destinations"]
    rebalance_to_idle_df = results["rebalance_to_idle"]
    weth_to_autopool = results["weth_to_autopool"]
    weth_from_autopool = results["weth_from_autopool"]

    UnderlyingDeposited_df = pd.concat([results[key] for key in results.keys() if "Underlying_deposited_" in key])
    UnderlyingWithdraw_df = pd.concat([results[key] for key in results.keys() if "Underlying_withdraw_" in key])

    return (
        rebalance_between_destinations_df,
        rebalance_to_idle_df,
        weth_to_autopool,
        weth_from_autopool,
        UnderlyingDeposited_df,
        UnderlyingWithdraw_df,
    )


def _combine_rebalance_event_data(
    autopool: AutopoolConstants,
    rebalance_between_destinations_df: pd.DataFrame,
    rebalance_to_idle_df: pd.DataFrame,
    weth_to_autopool: pd.DataFrame,
    weth_from_autopool: pd.DataFrame,
    UnderlyingDeposited_df: pd.DataFrame,
    UnderlyingWithdraw_df: pd.DataFrame,
):

    rebalance_between_destinations_df["outDestinationVault"] = rebalance_between_destinations_df[
        "outSummaryStats"
    ].apply(lambda x: Web3.toChecksumAddress(x[0]))

    rebalance_between_destinations_df["inDestinationVault"] = rebalance_between_destinations_df["inSummaryStats"].apply(
        lambda x: Web3.toChecksumAddress(x[0])
    )
    rebalance_to_idle_df["outDestinationVault"] = rebalance_to_idle_df["outSummary"].apply(
        lambda x: Web3.toChecksumAddress(x[0])
    )

    rebalance_to_idle_df["inDestinationVault"] = autopool.autopool_eth_addr

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

    rebalance_df = pd.concat([rebalance_to_idle_df, rebalance_between_destinations_df], axis=0)
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


def fetch_rebalance_events_and_actual_weth_and_lp_tokens_moved(autopool: AutopoolConstants, start_block: int):
    (
        rebalance_between_destinations_df,
        rebalance_to_idle_df,
        weth_to_autopool,
        weth_from_autopool,
        UnderlyingDeposited_df,
        UnderlyingWithdraw_df,
    ) = fetch_events_needed_for_rebalance_events(autopool, start_block)

    rebalance_df = _combine_rebalance_event_data(
        autopool,
        rebalance_between_destinations_df,
        rebalance_to_idle_df,
        weth_to_autopool,
        weth_from_autopool,
        UnderlyingDeposited_df,
        UnderlyingWithdraw_df,
    )
    rebalance_df = _add_spot_value_of_rebalance_events(rebalance_df, autopool)
    return rebalance_df


# # plots


def _make_rebalance_events_plots(clean_rebalance_df):
    figures = []
    figures.append(_add_composite_return_figures(clean_rebalance_df))
    figures.append(_add_in_out_eth_value(clean_rebalance_df))
    figures.append(_add_predicted_gain_and_swap_cost(clean_rebalance_df))
    figures.append(_add_swap_cost_percent(clean_rebalance_df))
    figures.append(_add_break_even_days_and_offset_period(clean_rebalance_df))
    return figures


def _add_composite_return_figures(clean_rebalance_df: pd.DataFrame) -> go.Figure:
    fig = go.Figure()
    fig.add_trace(
        go.Bar(x=clean_rebalance_df.index, y=clean_rebalance_df["out_compositeReturn"], name="Out Composite Return")
    )
    fig.add_trace(
        go.Bar(x=clean_rebalance_df.index, y=clean_rebalance_df["in_compositeReturn"], name="In Composite Return")
    )
    fig.update_yaxes(title_text="Return (%)")
    fig.update_layout(
        title="Composite Returns",
        bargap=0.0,
        bargroupgap=0.01,
    )
    return fig


def _add_in_out_eth_value(clean_rebalance_df: pd.DataFrame) -> go.Figure:
    fig = go.Figure()
    fig.add_trace(go.Bar(x=clean_rebalance_df.index, y=clean_rebalance_df["outEthValue"], name="Out ETH Value"))
    fig.add_trace(go.Bar(x=clean_rebalance_df.index, y=clean_rebalance_df["inEthValue"], name="In ETH Value"))
    fig.update_yaxes(title_text="ETH")
    fig.update_layout(
        title="In/Out ETH Values",
        bargap=0.0,
        bargroupgap=0.01,
    )
    return fig


def _add_predicted_gain_and_swap_cost(clean_rebalance_df: pd.DataFrame) -> go.Figure:
    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            x=clean_rebalance_df.index,
            y=clean_rebalance_df["predicted_gain_during_swap_cost_off_set_period"],
            name="Predicted Gain",
        )
    )
    fig.add_trace(go.Bar(x=clean_rebalance_df.index, y=clean_rebalance_df["swapCost"], name="Swap Cost"))
    fig.update_yaxes(title_text="ETH")
    fig.update_layout(title="Swap Cost and Predicted Gain", bargap=0.0, bargroupgap=0.01)
    return fig


def _add_swap_cost_percent(clean_rebalance_df: pd.DataFrame) -> go.Figure:
    swap_cost_percentage = clean_rebalance_df["slippage"] * 100
    fig = go.Figure()
    fig.add_trace(go.Bar(x=clean_rebalance_df.index, y=swap_cost_percentage, name="Swap Cost Percentage"))
    fig.update_yaxes(title_text="Swap Cost (%)")
    fig.update_layout(
        title="Swap Cost as Percentage of Out ETH Value",
        bargap=0.0,
        bargroupgap=0.01,
    )
    return fig


def _add_break_even_days_and_offset_period(clean_rebalance_df: pd.DataFrame) -> go.Figure:
    fig = go.Figure()
    fig.add_trace(go.Bar(x=clean_rebalance_df.index, y=clean_rebalance_df["break_even_days"], name="Break Even Days"))
    fig.add_trace(go.Bar(x=clean_rebalance_df.index, y=clean_rebalance_df["offset_period"], name="Offset Period"))
    fig.update_yaxes(title_text="Days")
    fig.update_layout(
        title="Break Even Days and Offset Period",
        bargap=0.0,
        bargroupgap=0.01,
    )
    return fig


def make_expoded_box_plot(df: pd.DataFrame, col: str, resolution: str = "1W"):
    # assumes df is timestmap index
    list_df = df.resample(resolution)[col].apply(list).reset_index()
    exploded_df = list_df.explode(col)

    return px.box(exploded_df, x="timestamp", y=col, title=f"Distribution of {col}")


if __name__ == "__main__":
    from mainnet_launch.constants.constants import AUTO_ETH
    from mainnet_launch.database.database_operations import drop_table

    # drop_table(REBALANCE_EVENTS_TABLE)

    new_rebalance_events_df = fetch_rebalance_events_df_from_external_source(
        AUTO_ETH, AUTO_ETH.chain.block_autopool_first_deployed
    )
    print(new_rebalance_events_df.head())
