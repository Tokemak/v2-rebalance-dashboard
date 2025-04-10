import pandas as pd
from multicall import Call

from mainnet_launch.constants import STATS_CALCULATOR_REGISTRY, ROOT_PRICE_ORACLE, ETH_CHAIN, ChainData
from mainnet_launch.abis import STATS_CALCULATOR_REGISTRY_ABI
from mainnet_launch.data_fetching.get_events import fetch_events
from mainnet_launch.data_fetching.get_state_by_block import (
    identity_with_bool_success,
    get_state_by_one_block,
    safe_normalize_with_bool_success,
)


def _fetch_lst_calc_addresses_df(chain: ChainData) -> pd.DataFrame:
    stats_calculator_registry_contract = ETH_CHAIN.client.eth.contract(
        STATS_CALCULATOR_REGISTRY(ETH_CHAIN), abi=STATS_CALCULATOR_REGISTRY_ABI
    )

    StatCalculatorRegistered = fetch_events(
        stats_calculator_registry_contract.events.StatCalculatorRegistered, ETH_CHAIN
    )

    lstTokenAddress_calls = [
        Call(
            a,
            ["lstTokenAddress()(address)"],
            [(a, identity_with_bool_success)],
        )
        for a in StatCalculatorRegistered["calculatorAddress"]
    ]

    calculator_to_lst_address = get_state_by_one_block(
        lstTokenAddress_calls, StatCalculatorRegistered["block"].max().astype("int"), chain=ETH_CHAIN
    )

    StatCalculatorRegistered["lst"] = StatCalculatorRegistered["calculatorAddress"].map(calculator_to_lst_address)
    lst_calcs = StatCalculatorRegistered[~StatCalculatorRegistered["lst"].isna()].copy()

    symbol_calls = [
        Call(
            a,
            ["symbol()(string)"],
            [(a, identity_with_bool_success)],
        )
        for a in lst_calcs["lst"]
    ]
    calculator_to_lst_address = get_state_by_one_block(symbol_calls, ETH_CHAIN.client.eth.block_number, ETH_CHAIN)
    lst_calcs["symbol"] = lst_calcs["lst"].map(calculator_to_lst_address)
    return lst_calcs[["lst", "symbol", "calculatorAddress"]]

def build_backing_calls():
    lst_calcs = _fetch_lst_calc_addresses_df(ETH_CHAIN)

    backing_calls = [
        Call(
            calculatorAddress,
            ["calculateEthPerToken()(uint256)"],
            [(f"{symbol}_backing", safe_normalize_with_bool_success)],
        )
        for (calculatorAddress, symbol) in zip(lst_calcs["calculatorAddress"], lst_calcs["symbol"])
    ]
    return backing_calls


def build_lst_safe_price_and_backing_calls() -> list[Call]:

    lst_calcs = _fetch_lst_calc_addresses_df(ETH_CHAIN)

    token_symbols_to_ignore = ["OETH", "stETH", "eETH"]
    # skip stETH and eETH because they are captured in wstETH and weETH
    # skip OETH because we dropped it in October 2024,
    lst_calcs = lst_calcs[~lst_calcs["symbol"].isin(token_symbols_to_ignore)].copy()

    safe_price_calls = [
        Call(
            ROOT_PRICE_ORACLE(ETH_CHAIN),
            ["getPriceInEth(address)(uint256)", lst],
            [(f"{symbol}_safe_price", safe_normalize_with_bool_success)],
        )
        for (lst, symbol) in zip(lst_calcs["lst"], lst_calcs["symbol"])
    ]

    backing_calls = [
        Call(
            calculatorAddress,
            ["calculateEthPerToken()(uint256)"],
            [(f"{symbol}_backing", safe_normalize_with_bool_success)],
        )
        for (calculatorAddress, symbol) in zip(lst_calcs["calculatorAddress"], lst_calcs["symbol"])
    ]

    return [*safe_price_calls, *backing_calls]
