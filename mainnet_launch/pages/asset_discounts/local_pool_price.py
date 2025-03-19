#

# Here is how the root price oracle currently works

# Pool of tokenA,tokenB

# put in 1 tokenA -> get some of Token B

# some of token B * tokenB safePrice == spot price of Token A in that pool

from multicall import Call
from dataclasses import dataclass

import pandas as pd
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
FRAX = "0xB9E1E3A9feFf48998E45Fa90847ed4D467E8BcfD"

crvUSD = "0xf939E0A03FB07F59A73314E73794Be0E57ac1b4E"
scrvUSD = "0x0655977FEb2f289A4aB78af67BAB0d17aAb84367"
sUSDs = "0xa3931d71877C0E7a3148CB7Eb4463524FEc27fbD"
sUSDe = "0x9D39A5DE30e57443BfF2A8307A4256c8797A3497"
sFRAX = "0xA663B02CF0a4b149d2aD41910CB81e23e1c41c32"

aUSDT = "0x7Bc3485026Ac48b6cf9BaF0A377477Fff5703Af8"
aUSDC = "0xD4fa2D31b7968E448877f69A96DE69f5de8cD23E"
aGHO = "0xC71Ea051a5F82c67ADcF634c36FFE6334793D24C"


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
        cleaning_function = _normalize_18_first_value

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


def build_safe_price_calls() -> list[Call]:
    return [
        make_chainlink_price_call(DAI_USD_chainlink, 18, "DAI_safe_price"),
        make_chainlink_price_call(USDe_USD_chainlink, 18, "USDe_safe_price"),
        make_chainlink_price_call(USDC_USD_chainlink, 6, "USDC_safe_price"),
        make_chainlink_price_call(USDT_USD_chainlik, 6, "USDT_safe_price"),
        make_chainlink_price_call(GHO_USD_chainlink, 18, "GHO_safe_price"),
        make_chainlink_price_call(crvUSD_USD_chainlink, 18, "crvUSD_safe_price"),
        make_chainlink_price_call(USDs_USD_chainlink, 18, "USDs_safe_price"),
        make_chainlink_price_call(FRAX_USD_chainlink, 18, "FRAX_safe_price"),
        make_chainlink_price_call(sUSDe_USD_chainlink, 18, "sUSDe_safe_price"),
    ]


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
    ]


@dataclass
class TokenLocalPoolPriceDetails:
    calls: list[Call]
    pool_name: str


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
                [("scrvUSD_sUSDe_pool__scrvUSD_to_sUSDe", safe_normalize_with_bool_success)],
            ),
            Call(
                "0xd29f8980852c2c76fC3f6E96a7Aa06E0BedCC1B1",
                ["get_dy(int128,int128,uint256)(uint256)", 0, 1, int(1e18)],
                [("scrvUSD_sUSDe_pool__sUSDe_to_scrvUSD", safe_normalize_with_bool_success)],
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

    return [
        crvUSD_USDC_pool,
        crvUSD_USDT_pool,
        crvUSD_GHO_pool,
        crvUSD_USDe_pool,
        crvUSD_FRAX_pool,
        USDe_DAI_pool,
        USDe_USDC_pool,
        crvUSD_sUSDe_pool,
    ]


def _build_balancer_pool_local_price() -> list[TokenLocalPoolPriceDetails]:

    GHO_USDC_USDT_v2_pool = TokenLocalPoolPriceDetails(
        calls=[
            build_balancer_query_swap_call(
                "0x8353157092ed8be69a9df8f95af097bbf33cb2af0000000000000000000005d9",
                GHO,
                USDC,
                int(1e18),
                "GHO_USDC_USDT_v2_GHO_to_USDC",
                6,
            ),
            build_balancer_query_swap_call(
                "0x8353157092ed8be69a9df8f95af097bbf33cb2af0000000000000000000005d9",
                GHO,
                USDC,
                int(1e6),
                "GHO_USDC_USDT_v2_USDC_to_GHO",
                18,
            ),
            build_balancer_query_swap_call(
                "0x8353157092ed8be69a9df8f95af097bbf33cb2af0000000000000000000005d9",
                GHO,
                USDT,
                int(1e18),
                "GHO_USDC_USDT_v2_GHO_to_USDT",
                6,
            ),
            build_balancer_query_swap_call(
                "0x8353157092ed8be69a9df8f95af097bbf33cb2af0000000000000000000005d9",
                USDT,
                GHO,
                int(1e6),
                "GHO_USDC_USDT_v2_USDT_to_GHO",
                18,
            ),
            build_balancer_query_swap_call(
                "0x8353157092ed8be69a9df8f95af097bbf33cb2af0000000000000000000005d9",
                USDC,
                USDT,
                int(1e6),
                "GHO_USDC_USDT_v2_USDC_to_USDT",
                18,
            ),
            build_balancer_query_swap_call(
                "0x8353157092ed8be69a9df8f95af097bbf33cb2af0000000000000000000005d9",
                USDT,
                USDC,
                int(1e6),
                "GHO_USDC_USDT_v2_USDT_to_USDC",
                18,
            ),
        ],
        pool_name="GHO_USDC_USDT_v2_pool",
    )

    GHO_USDC_USDT_boosted_pool = "0x85B2b559bC2D21104C4DEFdd6EFcA8A20343361D"
    # we don't ahve spot prices for the aaveWrapped version

    aGHO_to_aUSDC_spot_price_call = make_balancer_router_query(
        "GHO_USDC_USDT_boosted_pool__aGHO_to_aUSDC", GHO_USDC_USDT_boosted_pool, aGHO, aUSDC, 1e18
    )

    return [GHO_USDC_USDT_v2_pool]


def build_all_local_pool_prices():
    curve_pool_local_token_price = _build_curve_pool_local_price()
    balancer_pool_local_token_price = _build_balancer_pool_local_price()

    return [*curve_pool_local_token_price, *balancer_pool_local_token_price]


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
