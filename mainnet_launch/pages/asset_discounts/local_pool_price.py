#

# Here is how the root price oracle currently works

# Pool of tokenA,tokenB

# put in 1 tokenA -> get some of Token B

# some of token B * tokenB safePrice == spot price of Token A in that pool

from multicall import Call
from dataclasses import dataclass

import pandas as pd
import numpy as np

from mainnet_launch.data_fetching.get_state_by_block import (
    safe_normalize_6_with_bool_success,
    safe_normalize_with_bool_success,
    get_raw_state_by_blocks,
    build_blocks_to_use,
)
from mainnet_launch.constants import ETH_CHAIN


DAI = "0x6B175474E89094C44Da98b954EedeAC495271d0F"
USDC = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
USDT = "0xdAC17F958D2ee523a2206206994597C13D831ec7"
GHO = "0x40D16FC0246aD3160Ccc09B8D0D3A2cD28aE6C2f"
USDs = "0xdC035D45d973E3EC169d2276DDab16f1e407384F"
USDe = "0x4c9EDD5852cd905f086C759E8383e09bff1E68B3"
FRAX = "0x853d955aCEf822Db058eb8505911ED77F175b99e"

crvUSD = "0xf939E0A03FB07F59A73314E73794Be0E57ac1b4E"
scrvUSD = "0x0655977FEb2f289A4aB78af67BAB0d17aAb84367"
sUSDs = "0xa3931d71877C0E7a3148CB7Eb4463524FEc27fbD"
sUSDe = "0x9D39A5DE30e57443BfF2A8307A4256c8797A3497"
sFRAX = "0xA663B02CF0a4b149d2aD41910CB81e23e1c41c32"
sDAI = "0x83F20F44975D03b1b09e64809B757c47f942BEeA"

aUSDT = "0x7Bc3485026Ac48b6cf9BaF0A377477Fff5703Af8"
aUSDC = "0xD4fa2D31b7968E448877f69A96DE69f5de8cD23E"
aGHO = "0xC71Ea051a5F82c67ADcF634c36FFE6334793D24C"

stable_coin_tuples = [
    (DAI, "DAI"),
    (USDC, "USDC"),
    (USDT, "USDT"),
    (GHO, "GHO"),
    (USDs, "USDs"),
    (USDe, "USDe"),
    (FRAX, "FRAX"),
    (crvUSD, "crvUSD"),
    (scrvUSD, "scrvUSD"),
    (sUSDs, "sUSDs"),
    (sUSDe, "sUSDe"),
    (sFRAX, "sFRAX"),
    (aUSDT, "aUSDT"),
    (aUSDC, "aUSDC"),
    (aGHO, "aGHO"),
]


# chalink oracles
DAI_USD_chainlink = "0xAed0c38402a5d19df6E4c03F4E2DceD6e29c1ee9"
USDe_USD_chainlink = "0xa569d910839Ae8865Da8F8e70FfFb0cBA869F961"
USDC_USD_chainlink = "0x8fFfFfd4AfB6115b954Bd326cbe7B4BA576818f6"
USDT_USD_chainlik = "0x3E7d1eAB13ad0104d2750B8863b489D65364e32D"
GHO_USD_chainlink = "0x3f12643D3f6f874d39C2a4c9f2Cd6f2DbAC877FC"
crvUSD_USD_chainlink = "0xEEf0C605546958c1f899b6fB336C20671f9cD49F"
USDs_USD_chainlink = "0xfF30586cD0F29eD462364C7e81375FC0C71219b1"
FRAX_USD_chainlink = "0xB9E1E3A9feFf48998E45Fa90847ed4D467E8BcfD"
sUSDe_USD_chainlink = "0xFF3BC18cCBd5999CE63E788A1c250a88626aD099"

USDC_ETH_chainlink = "0x986b5E1e1755e3C2440e960477f25201B0a8bbD4"
DAI_ETH_chainlink = "0x773616E4d11A78F511299002da57A0a94577F1f4"
USDT_ETH_chainlink = "0xEe9F2375b4bdF6387aa8265dD4FB8F16512A1d46"


def _constant_1(*args):
    return 1.0


def make_dummy_1_call(name: str) -> Call:
    # Dummy call that always returns 1.0
    return Call(
        "0x0000000000000000000000000000000000000000",
        [
            "dummy()(uint256)",
        ],
        [(name, _constant_1)],
    )


def _chainlink_safe_normalize_with_bool_success(success: int, value: int):
    if success:
        return int(value[1]) / 1e8
    return None


def _chainlink_safe_normalize_6_with_bool_success(success: int, value: int):
    if success:
        return int(value[1]) / 1e8
    return None


def make_chainlink_price_call(chainlink_oracle: str, decimals: int, name: str):
    if decimals == 6:
        cleaning_function = _chainlink_safe_normalize_6_with_bool_success
    elif decimals == 18:
        cleaning_function = _chainlink_safe_normalize_with_bool_success

    return Call(
        chainlink_oracle,
        ["latestRoundData()((uint80,int128,uint256,uint256,uint80))"],
        [(f"{name}", cleaning_function)],
    )


def build_safe_price_calls() -> list[Call]:
    return [
        make_chainlink_price_call(DAI_USD_chainlink, 18, "DAI_to_USD_safe_price"),
        make_chainlink_price_call(USDe_USD_chainlink, 18, "USDe_to_USD_safe_price"),
        make_chainlink_price_call(USDC_USD_chainlink, 6, "USDC_to_USD_safe_price"),
        make_chainlink_price_call(USDT_USD_chainlik, 6, "USDT_to_USD_safe_price"),
        make_chainlink_price_call(GHO_USD_chainlink, 18, "GHO_to_USD_safe_price"),
        make_chainlink_price_call(crvUSD_USD_chainlink, 18, "crvUSD_to_USD_safe_price"),
        make_chainlink_price_call(USDs_USD_chainlink, 18, "USDs_to_USD_safe_price"),
        make_chainlink_price_call(FRAX_USD_chainlink, 18, "FRAX_to_USD_safe_price"),
        make_chainlink_price_call(sUSDe_USD_chainlink, 18, "sUSDe_to_USD_safe_price"),
        make_chainlink_price_call(USDC_ETH_chainlink, 18, "USDC_to_ETH_safe_price"),
        make_chainlink_price_call(USDT_ETH_chainlink, 18, "USDT_to_ETH_safe_price"),
        make_chainlink_price_call(DAI_ETH_chainlink, 18, "DAI_to_ETH_safe_price"),
    ]


def build_balancer_query_swap_call(
    pool_id: str, token_address_in: str, token_address_out: str, amount_in: int, name: str, decimals_out: int
):
    """
    Use the BalancerQueries contract to get the quote amount out

    Eg if I put amount_in of token 0 at pool_id how much of token Out will I get?


        interface IBalancerQueries {
    function querySwap(IVault.SingleSwap memory singleSwap, IVault.FundManagement memory funds)
        external
        returns (uint256);

    """

    balancer_queries_address = "0xE39B5e3B6D74016b2F6A9673D7d7493B6DF549d5"
    pool_id_as_bytes = bytes.fromhex(pool_id[2:])
    user_data = b""
    swap_kind = 0  # enum SwapKind { GIVEN_IN, GIVEN_OUT }
    zero_address = "0x0000000000000000000000000000000000000000"

    IVault_SingleSwap = (pool_id_as_bytes, swap_kind, token_address_in, token_address_out, amount_in, user_data)
    IVault_FundManagement = (zero_address, False, zero_address, False)

    if decimals_out == 6:
        cleaning_function = safe_normalize_6_with_bool_success
    elif decimals_out == 18:
        cleaning_function = safe_normalize_with_bool_success

    return Call(
        balancer_queries_address,
        [
            # Function signature for querySwap with two tuple parameters:
            "querySwap((bytes32,uint8,address,address,uint256,bytes),(address,bool,address,bool))(uint256)",
            IVault_SingleSwap,
            IVault_FundManagement,
        ],
        [
            (name, cleaning_function),
        ],
    )


def _normalize_6_first_value(success, amountOutList):
    if success:
        return amountOutList[0] / 1e6


def _normalize_18_first_value(success, amountOutList):
    if success:
        return amountOutList[0] / 1e18


def make_balancer_router_query(
    name,
    pool_address,
    token_in,
    token_out,
    amount_in,
    token_out_decimals,
):
    # simple one hop path
    balancer_batch_router_address = "0x136f1EFcC3f8f88516B9E94110D56FDBfB1778d1"
    paths = [
        (
            token_in,  # tokenIn
            [(pool_address, token_out, False)],  # steps as a list of SwapPathStep tuples
            int(amount_in),  # exactAmountIn
            0,  # minAmountOut
        )
    ]

    if token_out_decimals == 18:
        cleaning_function = _normalize_18_first_value
    elif token_out_decimals == 6:
        cleaning_function = _normalize_6_first_value

    return Call(
        balancer_batch_router_address,
        [
            "querySwapExactIn((address,(address,address,bool)[],uint256,uint256)[],address,bytes)(uint256[],address[],uint256[])",
            paths,
            "0x0000000000000000000000000000000000000000",
            b"",
        ],
        [
            (name, cleaning_function),
        ],
    )


def build_backing_calls() -> list[Call]:

    aUSDT_backing = Call(
        aUSDT,
        ["convertToAssets(uint256)(uint256)", int(1e18)],
        [("aUSDT_backing", safe_normalize_with_bool_success)],
    )

    aUSDC_backing = Call(
        aUSDC,
        ["convertToAssets(uint256)(uint256)", int(1e18)],
        [("aUSDC_backing", safe_normalize_with_bool_success)],
    )

    aGHO_backing = Call(
        aGHO,
        ["convertToAssets(uint256)(uint256)", int(1e18)],
        [("aGHO_backing", safe_normalize_with_bool_success)],
    )

    sUSDs_backing = Call(
        sUSDs,
        ["convertToAssets(uint256)(uint256)", int(1e18)],
        [("sUSDs_backing", safe_normalize_with_bool_success)],
    )

    sUSDe_backing = Call(
        sUSDe,
        ["convertToAssets(uint256)(uint256)", int(1e18)],
        [("sUSDe_backing", safe_normalize_with_bool_success)],
    )

    scrvUSD_backing = Call(
        scrvUSD,
        [
            "pricePerShare()(uint256)",
        ],
        [("scrvUSD_backing", safe_normalize_with_bool_success)],
    )

    sDAI_backing = Call(
        sDAI,
        ["convertToAssets(uint256)(uint256)", int(1e18)],
        [("sDAI_backing", safe_normalize_with_bool_success)],
    )

    sFRAX_backing = Call(
        sFRAX,
        ["pricePerShare()(uint256)"],
        [("sFRAX_backing", safe_normalize_with_bool_success)],
    )

    return [
        make_dummy_1_call("DAI_backing"),
        make_dummy_1_call("USDe_backing"),
        make_dummy_1_call("USDC_backing"),
        make_dummy_1_call("USDT_backing"),
        make_dummy_1_call("GHO_backing"),
        make_dummy_1_call("crvUSD_backing"),
        make_dummy_1_call("USDs_backing"),
        make_dummy_1_call("FRAX_backing"),
        aUSDT_backing,
        aUSDC_backing,
        aGHO_backing,
        sUSDs_backing,
        sUSDe_backing,
        scrvUSD_backing,
        sDAI_backing,
        sFRAX_backing,
    ]


@dataclass
class TokenLocalPoolPriceDetails:
    calls: list[Call]
    pool_name: str
    pool_address: str = None


def _build_curve_pool_local_price() -> list[TokenLocalPoolPriceDetails]:

    crvUSD_USDC_pool = TokenLocalPoolPriceDetails(
        calls=[
            Call(
                "0x4DEcE678ceceb27446b35C672dC7d61F30bAD69E",
                ["get_dy(int128,int128,uint256)(uint256)", 1, 0, int(1e18)],
                [("crvUSD_USDC_pool__crvUSD_to_USDC", safe_normalize_6_with_bool_success)],
            ),
            Call(
                "0x4DEcE678ceceb27446b35C672dC7d61F30bAD69E",
                ["get_dy(int128,int128,uint256)(uint256)", 0, 1, int(1e6)],
                [("crvUSD_USDC_pool__USDC_to_crvUSD", safe_normalize_with_bool_success)],
            ),
        ],
        pool_name="crvUSD_USDC_pool",
    )

    crvUSD_USDT_pool = TokenLocalPoolPriceDetails(
        calls=[
            Call(
                "0x390f3595bCa2Df7d23783dFd126427CCeb997BF4",
                ["get_dy(int128,int128,uint256)(uint256)", 0, 1, int(1e6)],
                [("crvUSD_USDT_pool__USDT_to_crvUSD", safe_normalize_with_bool_success)],
            ),
            Call(
                "0x390f3595bCa2Df7d23783dFd126427CCeb997BF4",
                ["get_dy(int128,int128,uint256)(uint256)", 1, 0, int(1e18)],
                [("crvUSD_USDT_pool__crvUSD_to_USDT", safe_normalize_6_with_bool_success)],
            ),
        ],
        pool_name="crvUSD_USDT_pool",
    )

    crvUSD_GHO_pool = TokenLocalPoolPriceDetails(
        calls=[
            Call(
                "0x635EF0056A597D13863B73825CcA297236578595",
                ["get_dy(int128,int128,uint256)(uint256)", 1, 0, int(1e18)],
                [("crvUSD_GHO_pool__GHO_to_crvUSD", safe_normalize_with_bool_success)],
            ),
            Call(
                "0x635EF0056A597D13863B73825CcA297236578595",
                ["get_dy(int128,int128,uint256)(uint256)", 0, 1, int(1e18)],
                [("crvUSD_GHO_pool__crvUSD_to_GHO", safe_normalize_with_bool_success)],
            ),
        ],
        pool_name="crvUSD_GHO_pool",
    )

    crvUSD_USDe_pool = TokenLocalPoolPriceDetails(
        calls=[
            Call(
                "0xF55B0f6F2Da5ffDDb104b58a60F2862745960442",
                ["get_dy(int128,int128,uint256)(uint256)", 1, 0, int(1e18)],
                [("crvUSD_USDe_pool__crvUSD_to_USDe", safe_normalize_with_bool_success)],
            ),
            Call(
                "0xF55B0f6F2Da5ffDDb104b58a60F2862745960442",
                ["get_dy(int128,int128,uint256)(uint256)", 0, 1, int(1e18)],
                [("crvUSD_USDe_pool__USDe_to_crvUSD", safe_normalize_with_bool_success)],
            ),
        ],
        pool_name="crvUSD_USDe_pool",
    )

    scrvUSD_sUSDe_pool = TokenLocalPoolPriceDetails(
        calls=[
            Call(
                "0xd29f8980852c2c76fC3f6E96a7Aa06E0BedCC1B1",
                ["get_dy(int128,int128,uint256)(uint256)", 1, 0, int(1e18)],
                [("scrvUSD_sUSDe_pool__sUSDe_to_scrvUSD", safe_normalize_with_bool_success)],
            ),
            Call(
                "0xd29f8980852c2c76fC3f6E96a7Aa06E0BedCC1B1",
                ["get_dy(int128,int128,uint256)(uint256)", 0, 1, int(1e18)],
                [("scrvUSD_sUSDe_pool__scrvUSD_to_sUSDe", safe_normalize_with_bool_success)],
            ),
        ],
        pool_name="scrvUSD_sUSDe_pool",
    )

    # don't have a scrvUSD safe price so we can't do this
    # I don't see a crvUSD-USDs pool but I do see a scrvUSD-sUSDs pool

    crvUSD_FRAX_pool = TokenLocalPoolPriceDetails(
        calls=[
            Call(
                "0x0CD6f267b2086bea681E922E19D40512511BE538",
                ["get_dy(int128,int128,uint256)(uint256)", 1, 0, int(1e18)],
                [("crvUSD_FRAX_pool__crvUSD_to_FRAX", safe_normalize_with_bool_success)],
            ),
            Call(
                "0x0CD6f267b2086bea681E922E19D40512511BE538",
                ["get_dy(int128,int128,uint256)(uint256)", 0, 1, int(1e18)],
                [("crvUSD_FRAX_pool__FRAX_to_crvUSD", safe_normalize_with_bool_success)],
            ),
        ],
        pool_name="crvUSD_FRAX_pool",
    )

    USDe_DAI_pool = TokenLocalPoolPriceDetails(
        calls=[
            Call(
                "0xF36a4BA50C603204c3FC6d2dA8b78A7b69CBC67d",
                ["get_dy(int128,int128,uint256)(uint256)", 1, 0, int(1e18)],
                [("USDe_DAI_pool__DAI_to_USDe", safe_normalize_with_bool_success)],
            ),
            Call(
                "0xF36a4BA50C603204c3FC6d2dA8b78A7b69CBC67d",
                ["get_dy(int128,int128,uint256)(uint256)", 0, 1, int(1e18)],
                [("USDe_DAI_pool__USDe_to_DAI", safe_normalize_with_bool_success)],
            ),
        ],
        pool_name="USDe_DAI_pool",
    )

    # https://curve.fi/dex/ethereum/pools/factory-stable-ng-12/deposit/ USDC-USDe

    USDe_USDC_pool = TokenLocalPoolPriceDetails(
        calls=[
            Call(
                "0x02950460E2b9529D0E00284A5fA2d7bDF3fA4d72",
                ["get_dy(int128,int128,uint256)(uint256)", 1, 0, int(1e6)],
                [("USDe_USDC_pool__USDC_to_USDe", safe_normalize_with_bool_success)],
            ),
            Call(
                "0x02950460E2b9529D0E00284A5fA2d7bDF3fA4d72",
                ["get_dy(int128,int128,uint256)(uint256)", 0, 1, int(1e18)],
                [("USDe_USDC_pool__USDe_to_USDC", safe_normalize_6_with_bool_success)],
            ),
        ],
        pool_name="USDe_USDC_pool",
    )

    crvUSD_sUSDe_pool = TokenLocalPoolPriceDetails(
        calls=[
            Call(
                "0x57064F49Ad7123C92560882a45518374ad982e85",
                ["get_dy(int128,int128,uint256)(uint256)", 1, 0, int(1e18)],
                [("crvUSD_sUSDe_pool__sUSDe_to_crvUSD", safe_normalize_with_bool_success)],
            ),
            Call(
                "0x57064F49Ad7123C92560882a45518374ad982e85",
                ["get_dy(int128,int128,uint256)(uint256)", 0, 1, int(1e18)],
                [("crvUSD_sUSDe_pool__crvUSD_to_sUSDe", safe_normalize_with_bool_success)],
            ),
        ],
        pool_name="crvUSD_sUSDe_pool",
    )

    sUSDe_sUSDs_pool = TokenLocalPoolPriceDetails(
        calls=[
            Call(
                "0x3CEf1AFC0E8324b57293a6E7cE663781bbEFBB79",
                ["get_dy(int128,int128,uint256)(uint256)", 1, 0, int(1e18)],
                [("sUSDe_sUSDs_pool__sUSDs_to_sUSDe", safe_normalize_with_bool_success)],
            ),
            Call(
                "0x3CEf1AFC0E8324b57293a6E7cE663781bbEFBB79",
                ["get_dy(int128,int128,uint256)(uint256)", 0, 1, int(1e18)],
                [("sUSDe_sUSDs_pool__sUSDe_to_sUSDs", safe_normalize_with_bool_success)],
            ),
        ],
        pool_name="sUSDe_sUSDs_pool",
    )

    scrvUSD_sUSDs_pool = TokenLocalPoolPriceDetails(
        calls=[
            Call(
                "0xfD1627E3f3469C8392C8c3A261D8F0677586e5e1",
                ["get_dy(int128,int128,uint256)(uint256)", 1, 0, int(1e18)],
                [("scrvUSD_sUSDs_pool__sUSDs_to_scrvUSD", safe_normalize_with_bool_success)],
            ),
            Call(
                "0xfD1627E3f3469C8392C8c3A261D8F0677586e5e1",
                ["get_dy(int128,int128,uint256)(uint256)", 0, 1, int(1e18)],
                [("scrvUSD_sUSDs_pool__scrvUSD_to_sUSDs", safe_normalize_with_bool_success)],
            ),
        ],
        pool_name="scrvUSD_sUSDs_pool",
    )

    sDAI_sUSDe_pool = TokenLocalPoolPriceDetails(
        calls=[
            Call(
                "0x167478921b907422F8E88B43C4Af2B8BEa278d3A",
                ["get_dy(int128,int128,uint256)(uint256)", 1, 0, int(1e18)],
                [("sDAI_sUSDe_pool__sUSDe_to_sDAI", safe_normalize_with_bool_success)],
            ),
            Call(
                "0x167478921b907422F8E88B43C4Af2B8BEa278d3A",
                ["get_dy(int128,int128,uint256)(uint256)", 0, 1, int(1e18)],
                [("sDAI_sUSDe_pool__sDAI_to_sUSDe", safe_normalize_with_bool_success)],
            ),
        ],
        pool_name="sDAI_sUSDe_pool",
    )

    DAI_USDC_USDT_pool = TokenLocalPoolPriceDetails(
        calls=[
            Call(
                "0xbEbc44782C7dB0a1A60Cb6fe97d0b483032FF1C7",
                ["get_dy(int128,int128,uint256)(uint256)", 0, 1, int(1e18)],
                [("DAI_USDC_USDT_pool__DAI_to_USDC", safe_normalize_6_with_bool_success)],
            ),
        ],
        pool_name="DAI_USDC_USDT_pool",
    )

    # NOTE frxUSD not FRAX

    FRAX_sUSDs_pool = TokenLocalPoolPriceDetails(
        calls=[
            Call(
                "0x81A2612F6dEA269a6Dd1F6DeAb45C5424EE2c4b7",
                ["get_dy(int128,int128,uint256)(uint256)", 0, 1, int(1e18)],
                [("FRAX_sUSDs_pool__FRAX_to_sUSDs", safe_normalize_with_bool_success)],
            ),
            Call(
                "0x81A2612F6dEA269a6Dd1F6DeAb45C5424EE2c4b7",
                ["get_dy(int128,int128,uint256)(uint256)", 1, 0, int(1e18)],
                [("FRAX_sUSDs_pool__sUSDs_to_FRAX", safe_normalize_with_bool_success)],
            ),
        ],
        pool_name="FRAX_sUSDs_pool",
    )

    #

    FRAX_USDC_pool = TokenLocalPoolPriceDetails(
        calls=[
            Call(
                "0xDcEF968d416a41Cdac0ED8702fAC8128A64241A2",
                ["get_dy(int128,int128,uint256)(uint256)", 0, 1, int(1e18)],
                [("FRAX_USDC_pool__FRAX_to_USDC", safe_normalize_6_with_bool_success)],
            ),
        ],
        pool_name="FRAX_USDC_pool",
    )

    return [
        crvUSD_USDC_pool,
        crvUSD_USDT_pool,
        crvUSD_GHO_pool,
        crvUSD_USDe_pool,
        crvUSD_FRAX_pool,
        USDe_DAI_pool,
        USDe_USDC_pool,
        crvUSD_sUSDe_pool,
        scrvUSD_sUSDe_pool,
        sUSDe_sUSDs_pool,
        scrvUSD_sUSDs_pool,
        sDAI_sUSDe_pool,
        # added later as spot checks
        DAI_USDC_USDT_pool,
        FRAX_sUSDs_pool,
        FRAX_USDC_pool,
    ]


def _build_balancer_pool_local_price() -> list[TokenLocalPoolPriceDetails]:

    # GHO_USDC_USDT_v2_pool = TokenLocalPoolPriceDetails(
    #     calls=[
    #         build_balancer_query_swap_call(
    #             "0x8353157092ed8be69a9df8f95af097bbf33cb2af0000000000000000000005d9",
    #             GHO,
    #             USDC,
    #             int(1e18),
    #             "GHO_USDC_USDT_v2_GHO_to_USDC",
    #             6,
    #         ),
    #         build_balancer_query_swap_call(
    #             "0x8353157092ed8be69a9df8f95af097bbf33cb2af0000000000000000000005d9",
    #             GHO,
    #             USDC,
    #             int(1e6),
    #             "GHO_USDC_USDT_v2_USDC_to_GHO",
    #             18,
    #         ),
    #         # build_balancer_query_swap_call(
    #         #     "0x8353157092ed8be69a9df8f95af097bbf33cb2af0000000000000000000005d9",
    #         #     GHO,
    #         #     USDT,
    #         #     int(1e18),
    #         #     "GHO_USDC_USDT_v2_GHO_to_USDT",
    #         #     6,
    #         # ),
    #         # build_balancer_query_swap_call(
    #         #     "0x8353157092ed8be69a9df8f95af097bbf33cb2af0000000000000000000005d9",
    #         #     USDT,
    #         #     GHO,
    #         #     int(1e6),
    #         #     "GHO_USDC_USDT_v2_USDT_to_GHO",
    #         #     18,
    #         # ),
    #         build_balancer_query_swap_call(
    #             "0x8353157092ed8be69a9df8f95af097bbf33cb2af0000000000000000000005d9",
    #             USDC,
    #             USDT,
    #             int(1e6),
    #             "GHO_USDC_USDT_v2_USDC_to_USDT",
    #             18,
    #         ),
    #         build_balancer_query_swap_call(
    #             "0x8353157092ed8be69a9df8f95af097bbf33cb2af0000000000000000000005d9",
    #             USDT,
    #             USDC,
    #             int(1e6),
    #             "GHO_USDC_USDT_v2_USDT_to_USDC",
    #             18,
    #         ),
    #     ],
    #     pool_name="GHO_USDC_USDT_v2",
    # )

    GHO_USDC_USDT_boosted_pool_address = "0x85B2b559bC2D21104C4DEFdd6EFcA8A20343361D"

    GHO_USDC_USDT_boosted_pool = TokenLocalPoolPriceDetails(
        calls=[
            make_balancer_router_query(
                "GHO_USDC_USDT_boosted__aGHO_to_aUSDC", GHO_USDC_USDT_boosted_pool_address, aGHO, aUSDC, 1e18, 6
            ),
            make_balancer_router_query(
                "GHO_USDC_USDT_boosted__aUSDC_to_aGHO", GHO_USDC_USDT_boosted_pool_address, aUSDC, aGHO, 1e6, 18
            ),
            make_balancer_router_query(
                "GHO_USDC_USDT_boosted__aUSDT_to_aUSDC", GHO_USDC_USDT_boosted_pool_address, aUSDT, aUSDC, 1e6, 6
            ),
        ],
        pool_name="GHO_USDC_USDT_boosted",
        pool_address=GHO_USDC_USDT_boosted_pool_address,
    )

    sUSDe_USDC_balancer_pool = TokenLocalPoolPriceDetails(
        calls=[
            build_balancer_query_swap_call(
                "0xb819feef8f0fcdc268afe14162983a69f6bf179e000000000000000000000689",
                sUSDe,
                USDC,
                int(1e18),
                "sUSDe_USDC_balancer_pool__sUSDe_to_USDC",
                6,
            ),
            build_balancer_query_swap_call(
                "0xb819feef8f0fcdc268afe14162983a69f6bf179e000000000000000000000689",
                USDC,
                sUSDe,
                int(1e6),
                "sUSDe_USDC_balancer_pool__USDC_to_sUSDe",
                18,
            ),
        ],
        pool_name="sUSDe_USDC_balancer_pool",
        pool_address="0xb819feeF8F0fcDC268AfE14162983A69f6BF179E",
    )

    return [GHO_USDC_USDT_boosted_pool, sUSDe_USDC_balancer_pool]


def build_safe_to_usdc_token_price(blocks: list[int], method: str) -> pd.DataFrame:

    safe_price_calls = build_safe_price_calls()
    backing_calls = build_backing_calls()

    raw_df = get_raw_state_by_blocks([*safe_price_calls, *backing_calls], blocks, ETH_CHAIN)

    safe_to_usdc_price_df = pd.DataFrame(index=raw_df.index)
    if method == "(Token-ETH) / (USDC-ETH)":
        safe_to_usdc_price_df["DAI_safe_price"] = raw_df["DAI_to_ETH_safe_price"] / raw_df["USDC_to_ETH_safe_price"]
        safe_to_usdc_price_df["USDT_safe_price"] = raw_df["USDT_to_ETH_safe_price"] / raw_df["USDC_to_ETH_safe_price"]

    elif method == "(Token-USD) / (USDC-USD)":
        safe_to_usdc_price_df["DAI_safe_price"] = raw_df["DAI_to_USD_safe_price"] / raw_df["USDC_to_USD_safe_price"]
        safe_to_usdc_price_df["USDT_safe_price"] = raw_df["USDT_to_USD_safe_price"] / raw_df["USDC_to_USD_safe_price"]
    else:
        raise ValueError("invalid method")

    safe_to_usdc_price_df["USDe_safe_price"] = raw_df["USDe_to_USD_safe_price"] / raw_df["USDC_to_USD_safe_price"]
    safe_to_usdc_price_df["GHO_safe_price"] = raw_df["GHO_to_USD_safe_price"] / raw_df["USDC_to_USD_safe_price"]
    safe_to_usdc_price_df["crvUSD_safe_price"] = raw_df["crvUSD_to_USD_safe_price"] / raw_df["USDC_to_USD_safe_price"]
    safe_to_usdc_price_df["USDs_safe_price"] = raw_df["USDs_to_USD_safe_price"] / raw_df["USDC_to_USD_safe_price"]
    safe_to_usdc_price_df["FRAX_safe_price"] = raw_df["FRAX_to_USD_safe_price"] / raw_df["USDC_to_USD_safe_price"]
    safe_to_usdc_price_df["sUSDe_safe_price"] = raw_df["sUSDe_to_USD_safe_price"] / raw_df["USDC_to_USD_safe_price"]
    safe_to_usdc_price_df["USDC_safe_price"] = 1.0

    safe_to_usdc_price_df["scrvUSD_safe_price"] = safe_to_usdc_price_df["crvUSD_safe_price"] * raw_df["scrvUSD_backing"]
    safe_to_usdc_price_df["sDAI_safe_price"] = safe_to_usdc_price_df["DAI_safe_price"] * raw_df["sDAI_backing"]
    safe_to_usdc_price_df["sUSDs_safe_price"] = safe_to_usdc_price_df["USDs_safe_price"] * raw_df["sUSDs_backing"]
    safe_to_usdc_price_df["sFRAX_safe_price"] = safe_to_usdc_price_df["FRAX_safe_price"] * raw_df["sFRAX_backing"]

    # balancer boosted pools
    safe_to_usdc_price_df["aGHO_safe_price"] = safe_to_usdc_price_df["GHO_safe_price"] * raw_df["aGHO_backing"]
    safe_to_usdc_price_df["aUSDC_safe_price"] = safe_to_usdc_price_df["USDC_safe_price"] * raw_df["aUSDC_backing"]
    safe_to_usdc_price_df["aUSDT_safe_price"] = safe_to_usdc_price_df["USDT_safe_price"] * raw_df["aUSDT_backing"]

    return safe_to_usdc_price_df


def build_all_local_pool_prices():
    curve_pool_local_token_price = _build_curve_pool_local_price()
    balancer_pool_local_token_price = _build_balancer_pool_local_price()

    return [*curve_pool_local_token_price, *balancer_pool_local_token_price]


def _build_curve_swap_fee_calls() -> list[Call]:
    curve_pools = _build_curve_pool_local_price()
    token_local_price_call_list = [t.calls for t in curve_pools]
    token_local_price_calls = [c for calls in token_local_price_call_list for c in calls]
    FEE_PRECISION = 1e10

    def _compute_fee_as_portion(success: bool, fee: int):
        if success:
            return fee / FEE_PRECISION
        return np.nan

    pool_name_tuples = []
    for c in token_local_price_calls:
        pool_address = c.target
        pool_name = c.returns[0][0].split("__")[0]

        pool_name_tuples.append((pool_name, pool_address))
    pool_name_tuples = set(pool_name_tuples)
    pool_name_tuples

    curve_fee_calls = [
        Call(
            pool,
            ["fee()(uint256)"],
            [(f"{pool_name}_fee", _compute_fee_as_portion)],
        )
        for pool_name, pool in pool_name_tuples
    ]
    return curve_fee_calls


def _build_balancer_swap_fee_calls() -> list[Call]:

    FEE_PRECISION = 1e18

    def _compute_fee_as_portion(success: bool, fee: int):
        if success:
            return fee / FEE_PRECISION
        return np.nan

    # def _compute_fee_as_portion_agg(success: bool,args):
    #     aggregateSwapFeePercentage, aggregateYieldFeePercentage = args
    #     # return args
    #     if success:
    #         return aggregateSwapFeePercentage / FEE_PRECISION
    #     return np.nan

    balancer_fee_calls = [
        Call(
            "0x85B2b559bC2D21104C4DEFdd6EFcA8A20343361D",
            ["getStaticSwapFeePercentage()(uint256)"],
            [("GHO_USDC_USDT_boosted_fee", _compute_fee_as_portion)],
        ),
        Call(
            "0xb819feeF8F0fcDC268AfE14162983A69f6BF179E",
            ["getSwapFeePercentage()(uint256)"],
            [("sUSDe_USDC_balancer_pool_fee", _compute_fee_as_portion)],
        ),
    ]

    return balancer_fee_calls


def build_fee_calls() -> list[Call]:
    curve_fee_calls = _build_curve_swap_fee_calls()
    balancer_fee_calls = _build_balancer_swap_fee_calls()
    return [*curve_fee_calls, *balancer_fee_calls]


# def _build_pool_price_calls():

#     DAI_USDe_pool = ""
#     crvUSD_USDC_pool = ""
#     crvUSD_USDT_pool = ""
#     crvUSD_USDe_pool = ""
#     crvUSD_USDS_pool = "" # this pool exists but has no liqudity
#     GHO_crvUSD_pool = ""
#     FRAX_crvUSD_pool = ""
#     crvUSD_sUSDe_pool = ""
#     USDT_GHO_USDC_pool = "" # bal pool
#     USDe_USDC_pool = ""https://balancer.fi/pools/ethereum/v2/0xb819feef8f0fcdc268afe14162983a69f6bf179e000000000000000000000689
#     GHO_USDC_pool = ""
# GYD_USDC_pool = "" skip
