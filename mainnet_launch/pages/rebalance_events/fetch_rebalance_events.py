import pandas as pd
from multicall import Call
from web3 import Web3

from mainnet_launch.constants import (
    AutopoolConstants,
    ROOT_PRICE_ORACLE,
    ChainData,
    ALL_AUTOPOOLS,
    WETH,
)
from mainnet_launch.abis import (
    ROOT_PRICE_ORACLE_ABI,
    AUTOPOOL_ETH_STRATEGY_ABI,
    ERC_20_ABI,
    BALANCER_AURA_DESTINATION_VAULT_ABI,
)
from mainnet_launch.data_fetching.get_events import fetch_events
from mainnet_launch.data_fetching.get_state_by_block import (
    get_state_by_one_block,
    get_raw_state_by_blocks,
    safe_normalize_with_bool_success,
    identity_with_bool_success,
)
from mainnet_launch.destinations import get_destination_details

from mainnet_launch.data_fetching.add_info_to_dataframes import (
    add_timestamp_to_df_with_block_column,
    add_transaction_gas_info_to_df_with_tx_hash,
)

from mainnet_launch.database.new_databases import (
    write_dataframe_to_table,
    run_read_only_query,
    get_earliest_block_from_table_with_autopool,
)


from mainnet_launch.database.should_update_database import (
    should_update_table,
)


REBALANCE_EVENTS_TABLE = "REBALANCE_EVENTS_TABLE"


def add_new_rebalance_events_for_each_autopool_to_table():
    for autopool in ALL_AUTOPOOLS:
        highest_block_already_fetched = get_earliest_block_from_table_with_autopool(REBALANCE_EVENTS_TABLE, autopool)
        new_rebalance_events_df = fetch_rebalance_events_df_from_external_source(
            autopool, highest_block_already_fetched
        )
        write_dataframe_to_table(new_rebalance_events_df, REBALANCE_EVENTS_TABLE)


def fetch_rebalance_events_df(autopool: AutopoolConstants) -> pd.DataFrame:

    if should_update_table(REBALANCE_EVENTS_TABLE):
        add_new_rebalance_events_for_each_autopool_to_table()

    query = f"""
        SELECT * from {REBALANCE_EVENTS_TABLE}
        
        WHERE autopool = ?
        
        """
    params = (autopool.name,)
    rebalance_events_df = run_read_only_query(query, params)
    rebalance_events_df = rebalance_events_df.set_index("timestamp")
    return rebalance_events_df


def fetch_rebalance_events_df_from_external_source(autopool: AutopoolConstants, start_block: int) -> pd.DataFrame:
    clean_rebalance_df = fetch_and_clean_rebalance_between_destination_events(autopool, start_block)

    clean_rebalance_df["flash_borrower_address"] = clean_rebalance_df.apply(
        lambda row: _get_flash_borrower_address(row["hash"], autopool.chain), axis=1
    )

    clean_rebalance_df = _add_solver_profit_cols(clean_rebalance_df, autopool)

    clean_rebalance_df = add_timestamp_to_df_with_block_column(clean_rebalance_df, autopool.chain)
    clean_rebalance_df["timestamp"] = clean_rebalance_df.index

    clean_rebalance_df["autopool"] = autopool.name
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


def fetch_and_clean_rebalance_between_destination_events(autopool: AutopoolConstants, start_block: int) -> pd.DataFrame:
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

    return clean_rebalance_df


def getPriceInEth_call(name: str, token_address: str, chain: ChainData) -> Call:
    return Call(
        ROOT_PRICE_ORACLE(chain),
        ["getPriceInEth(address)(uint256)", token_address],
        [(name, safe_normalize_with_bool_success)],
    )


def _get_flash_borrower_address(tx_hash: str, chain: ChainData) -> str:
    # get the address of the flash borrower that did this rebalance
    return chain.client.eth.get_transaction(tx_hash)["to"]


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
    tokens: list[str] = fetch_events(root_price_oracle_contract.events.TokenRegistered)["token"].values

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


def _fetch_destination_UnderlyingDeposited(autopool: AutopoolConstants, start_block: int) -> pd.DataFrame:
    destinations = get_destination_details(autopool)

    vaultAddresses = list(set([d.vaultAddress for d in destinations]))
    dfs = []

    for vault_address in vaultAddresses:
        contract = autopool.chain.client.eth.contract(
            Web3.toChecksumAddress(vault_address), abi=BALANCER_AURA_DESTINATION_VAULT_ABI
        )
        df = fetch_events(contract.events.UnderlyingDeposited, start_block=start_block)
        df["contract_address"] = contract.address
        dfs.append(df)

    UnderlyingDeposited_df = pd.concat(dfs, axis=0)
    return UnderlyingDeposited_df


def _fetch_destination_UnderlyingWithdraw(autopool: AutopoolConstants, start_block: int) -> pd.DataFrame:
    destinations = get_destination_details(autopool)
    vaultAddresses = list(set([d.vaultAddress for d in destinations]))
    dfs = []

    for vault_address in vaultAddresses:
        contract = autopool.chain.client.eth.contract(
            Web3.toChecksumAddress(vault_address), abi=BALANCER_AURA_DESTINATION_VAULT_ABI
        )
        df = fetch_events(contract.events.UnderlyingWithdraw, start_block=start_block)
        df["contract_address"] = contract.address
        dfs.append(df)

    UnderlyingWithdraw_df = pd.concat(dfs, axis=0)
    return UnderlyingWithdraw_df


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


def _fetch_weth_transfers_to_or_from_autopool_vault(autopool: AutopoolConstants, start_block: int) -> pd.DataFrame:

    weth_contract = autopool.chain.client.eth.contract(WETH(autopool.chain), abi=ERC_20_ABI)

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


def fetch_rebalance_events_and_actual_weth_and_lp_tokens_moved(
    autopool: AutopoolConstants, start_block: int
) -> pd.DataFrame:

    strategy_contract = autopool.chain.client.eth.contract(
        autopool.autopool_eth_strategy_addr, abi=AUTOPOOL_ETH_STRATEGY_ABI
    )

    rebalance_between_destinations_df = fetch_events(
        strategy_contract.events.RebalanceBetweenDestinations, start_block=start_block
    )

    rebalance_between_destinations_df["outDestinationVault"] = rebalance_between_destinations_df[
        "outSummaryStats"
    ].apply(lambda x: Web3.toChecksumAddress(x[0]))

    rebalance_between_destinations_df["inDestinationVault"] = rebalance_between_destinations_df["inSummaryStats"].apply(
        lambda x: Web3.toChecksumAddress(x[0])
    )

    rebalance_to_idle_df = fetch_events(strategy_contract.events.RebalanceToIdle, start_block=start_block)

    rebalance_to_idle_df["outDestinationVault"] = rebalance_to_idle_df["outSummary"].apply(
        lambda x: Web3.toChecksumAddress(x[0])
    )

    rebalance_to_idle_df["inDestinationVault"] = autopool.autopool_eth_addr
    rebalance_df = pd.concat([rebalance_to_idle_df, rebalance_between_destinations_df], axis=0)

    weth_to_autopool, weth_from_autopool = _fetch_weth_transfers_to_or_from_autopool_vault(
        autopool, start_block=start_block
    )
    UnderlyingDeposited_df = _fetch_destination_UnderlyingDeposited(autopool, start_block=start_block)
    UnderlyingWithdraw_df = _fetch_destination_UnderlyingWithdraw(autopool, start_block=start_block)

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

    # amount_deposited the quantity of LP tokens deposited to a destination (value out the autopool)
    # amount_withdrawn the quantity of LP tokens withdrawn from a destination (value into the autopool)
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
    rebalance_df = _add_spot_value_of_rebalance_events(rebalance_df, autopool)
    return rebalance_df


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
