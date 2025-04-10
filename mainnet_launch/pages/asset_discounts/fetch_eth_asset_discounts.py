import pandas as pd
from multicall import Call

from mainnet_launch.constants import STATS_CALCULATOR_REGISTRY, ROOT_PRICE_ORACLE, ETH_CHAIN, ChainData
from mainnet_launch.abis import STATS_CALCULATOR_REGISTRY_ABI
from mainnet_launch.data_fetching.get_events import fetch_events
from mainnet_launch.data_fetching.get_state_by_block import (
    identity_with_bool_success,
    get_state_by_one_block,
    safe_normalize_with_bool_success,
    make_dummy_1_call,
)

cbETH = "0xbe9895146f7af43049ca1c1ae358b0541ea49704"
eETH = "0x35fa164735182de50811e8e2e824cfb9b6118ac2"
ETHx = "0xa35b1b31ce002fbf2058d22f30f95d405200a15b"
ezETH = "0xbf5495efe5db9ce00f80364c8b423567e58d2110"
frxETH = "0x5e8422345238f34275888049021821e8e08caa1f"
osETH = "0xf1c9acdc66974dfb6decb12aa385b9cd01190e38"
pxETH = "0x04c154b66cb340f3ae24111cc767e0184ed00cc6"
rETH = "0xae78736cd615f374d3085123a210448e74fc6393"
rsETH = "0xa1290d69c65a6fe4df752f95823fae25cb99e5a7"
rswETH = "0xfae103dc9cf190ed75350761e95403b7b8afa6c0"
stETH = "0xae7ab96520de3a18e5e111b5eaab095312d7fe84"
swETH = "0xf951e335afb289353dc249e82926178eac7ded78"
weETH = "0xcd5fe23c85820f7b72d0926fc9b05b43e359b7ee"
wstETH = "0x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0"
OETH = "0x856c4efb76c1d1ae02e20ceb03a2a6a08b0b8dc3"


lst_tuples = [
    (cbETH, "cbETH"),
    (eETH, "eETH"),
    (ETHx, "ETHx"),
    (ezETH, "ezETH"),
    (frxETH, "frxETH"),
    (osETH, "osETH"),
    (pxETH, "pxETH"),
    (rETH, "rETH"),
    (rsETH, "rsETH"),
    (rswETH, "rswETH"),
    (stETH, "stETH"),
    (swETH, "swETH"),
    (weETH, "weETH"),
    (wstETH, "wstETH"),
    (OETH, "OETH"),
]


def build_lst_backing_calls():

    cbETH_backing = Call(
        cbETH,
        [
            "exchangeRate()(uint256)",
        ],
        [("cbETH_backing", safe_normalize_with_bool_success)],
    )

    eETH_backing = make_dummy_1_call("eETH_backing")

    # cbETH_backing = Call(
    #     cbETH,
    #     ["convertToAssets(uint256)(uint256)", int(1e18)],
    #     [("cbETH_backing", safe_normalize_with_bool_success)],
    # )

    return [cbETH_backing, make_dummy_1_call("stETH_backing")]


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
    p

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


if __name__ == "__main__":
    build_lst_safe_price_and_backing_calls()
