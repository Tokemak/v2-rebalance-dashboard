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
WETH = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
ETH_0 = "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
WETH_on_base = "0x4200000000000000000000000000000000000006"

cbETH_lst_calculator = "0xaB78a570252dd06FDbC1C5C566E842e571d01E08"
eETH_lst_calculator = "0x4353e181C13f7E970f24016a0762C1af271350BA"
ETHx_lst_calculator = "0x6D3C5F6670ABe46901De4BD39036cF21d178334C"
ezETH_lst_calculator = "0xA84cc1d5aD1cDD5fAeb15Aa3f4aC5935D4b263D9"
frxETH_lst_calculator = "0x449A957490e24e4d915fD5Dcf25Dd5446E787590"
osETH_lst_calculator = "0xB5aa595C4FE3C297D65bdCDCc6FA48eF8725AEbB"
pxETH_lst_calculator = "0x9cB562083D29e027F21fAc4D8b66573deA972153"
rETH_lst_calculator = "0x9801098EE481ed6806C61A4dE259FBdDD5bb84a8"
rsETH_lst_calculator = "0x840A49a4b83E57718cf67c03D820C938A04FC210"
rswETH_lst_calculator = "0xeEdb3dD86F690a8c76006D606Db7951322B6741A"
stETH_lst_calculator = "0x66A466b838f981B39cF3B3E13E19AF5643Dbad0c"
swETH_lst_calculator = "0x60E98E2dAc20FAab84781076164290Cc31Ce3c9e"
weETH_lst_calculator = "0xDBFB637873D16DC5eFa43DB75Ff846934CaAA43f"
wstETH_lst_calculator = "0x24864cc03EFD84f9DF0e5F1D23aB69128325931E"
OETH_lst_calculator = "0x08ac17D02ca049De040d43DF19d1304b0B5fBAFb"


# Token address, symbol, lst calculator
lst_tuples = [
    (cbETH, "cbETH", cbETH_lst_calculator),
    (eETH, "eETH", eETH_lst_calculator),
    (ETHx, "ETHx", ETHx_lst_calculator),
    (ezETH, "ezETH", ezETH_lst_calculator),
    (frxETH, "frxETH", frxETH_lst_calculator),
    (osETH, "osETH", osETH_lst_calculator),
    (pxETH, "pxETH", pxETH_lst_calculator),
    (rETH, "rETH", rETH_lst_calculator),
    (rsETH, "rsETH", rsETH_lst_calculator),
    (rswETH, "rswETH", rswETH_lst_calculator),
    (stETH, "stETH", stETH_lst_calculator),
    (swETH, "swETH", swETH_lst_calculator),
    (weETH, "weETH", weETH_lst_calculator),
    (wstETH, "wstETH", weETH_lst_calculator),
    (OETH, "OETH", OETH_lst_calculator),
    (WETH, "WETH", None),
    (ETH_0, "ETH", None),
    (WETH_on_base, "WETH", None),
]


def build_lst_backing_calls():
    backing_calls = [
        Call(
            calc,
            ["calculateEthPerToken()(uint256)"],
            [(f"{symbol}_backing", safe_normalize_with_bool_success)],
        )
        for token, symbol, calc in lst_tuples
        if calc is not None
    ]
    return [*backing_calls, make_dummy_1_call("WETH_backing"), make_dummy_1_call("ETH_backing")]


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
    print(lst_calcs[["symbol", "calculatorAddress"]])
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


#                                            lst  symbol                           calculatorAddress
# 0   0xbe9895146f7af43049ca1c1ae358b0541ea49704   cbETH  0xaB78a570252dd06FDbC1C5C566E842e571d01E08
# 1   0x35fa164735182de50811e8e2e824cfb9b6118ac2    eETH  0x4353e181C13f7E970f24016a0762C1af271350BA
# 2   0xa35b1b31ce002fbf2058d22f30f95d405200a15b    ETHx  0x6D3C5F6670ABe46901De4BD39036cF21d178334C
# 3   0xbf5495efe5db9ce00f80364c8b423567e58d2110   ezETH  0xA84cc1d5aD1cDD5fAeb15Aa3f4aC5935D4b263D9
# 4   0x5e8422345238f34275888049021821e8e08caa1f  frxETH  0x449A957490e24e4d915fD5Dcf25Dd5446E787590
# 5   0xf1c9acdc66974dfb6decb12aa385b9cd01190e38   osETH  0xB5aa595C4FE3C297D65bdCDCc6FA48eF8725AEbB
# 6   0x04c154b66cb340f3ae24111cc767e0184ed00cc6   pxETH  0x9cB562083D29e027F21fAc4D8b66573deA972153
# 7   0xae78736cd615f374d3085123a210448e74fc6393    rETH  0x9801098EE481ed6806C61A4dE259FBdDD5bb84a8
# 8   0xa1290d69c65a6fe4df752f95823fae25cb99e5a7   rsETH  0x840A49a4b83E57718cf67c03D820C938A04FC210
# 9   0xfae103dc9cf190ed75350761e95403b7b8afa6c0  rswETH  0xeEdb3dD86F690a8c76006D606Db7951322B6741A
# 10  0xae7ab96520de3a18e5e111b5eaab095312d7fe84   stETH  0x66A466b838f981B39cF3B3E13E19AF5643Dbad0c
# 11  0xf951e335afb289353dc249e82926178eac7ded78   swETH  0x60E98E2dAc20FAab84781076164290Cc31Ce3c9e
# 12  0xcd5fe23c85820f7b72d0926fc9b05b43e359b7ee   weETH  0xDBFB637873D16DC5eFa43DB75Ff846934CaAA43f
# 13  0x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0  wstETH  0x24864cc03EFD84f9DF0e5F1D23aB69128325931E
# # 52  0x856c4efb76c1d1ae02e20ceb03a2a6a08b0b8dc3    OETH  0x08ac17D02ca049De040d43DF19d1304b0B5fBAFb

# def _handle_ETHx_getExchangeRate(success, args):
#     if success:
#         reportingBlockNumber, totalETHBalance, totalETHXSupply = args
#         return int(totalETHBalance) / int(totalETHXSupply)


# # cbETH_backing = Call(
# #     cbETH,
#     [
#         "exchangeRate()(uint256)",
#     ],
#     [("cbETH_backing", safe_normalize_with_bool_success)],
# )

# #  token.staderConfig().getStaderOracle().getExchangeRate();


# ETHx_backing = Call(
#         '0xF64bAe65f6f2a5277571143A24FaaFDFC0C2a737', # ETHx.staderConfig().getStaderOracle().getExchangeRate();
#         ["getExchangeRate()((uint256,uint256,uint256))"],
#         [("ETHx_backing", _handle_ETHx_getExchangeRate)],
#     )

# eETH_backing = make_dummy_1_call("eETH_backing")

# ezETH_backing =  Call(
#         '0xA84cc1d5aD1cDD5fAeb15Aa3f4aC5935D4b263D9',
#         ["calculateEthPerToken()(uint256)"],
#         [(f"ezETH_backing", safe_normalize_with_bool_success)],
#     )

# #0x6D3C5F6670ABe46901De4BD39036cF21d178334C


# # cbETH_backing = Call(
# #     cbETH,
# #     ["convertToAssets(uint256)(uint256)", int(1e18)],
# #     [("cbETH_backing", safe_normalize_with_bool_success)],
# # )

# return [cbETH_backing, make_dummy_1_call("stETH_backing")]
