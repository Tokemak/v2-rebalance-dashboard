import black
from multicall import Call
from dataclasses import dataclass
from mainnet_launch.data_fetching.get_state_by_block import (
    safe_normalize_6_with_bool_success,
    safe_normalize_with_bool_success,
)


DAI = "0x6B175474E89094C44Da98b954EedeAC495271d0F"
USDC = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
USDT = "0xdAC17F958D2ee523a2206206994597C13D831ec7"
GHO = "0x40D16FC0246aD3160Ccc09B8D0D3A2cD28aE6C2f"
USDs = "0xdC035D45d973E3EC169d2276DDab16f1e407384F"  # formerly DAI, and 1:1 convertable with dai?
USDe = "0x4c9EDD5852cd905f086C759E8383e09bff1E68B3"

crvUSD = "0xf939E0A03FB07F59A73314E73794Be0E57ac1b4E"
scrvUSD = "0x0655977FEb2f289A4aB78af67BAB0d17aAb84367"
sUSDs = "0xa3931d71877C0E7a3148CB7Eb4463524FEc27fbD"
sUSDe = "0x9D39A5DE30e57443BfF2A8307A4256c8797A3497"


# chalink oracles
DAI_USD_chainlink = "0xAed0c38402a5d19df6E4c03F4E2DceD6e29c1ee9"
USDe_USD_chainlink = "0xa569d910839Ae8865Da8F8e70FfFb0cBA869F961"
USDC_USD_chainlink = "0x8fFfFfd4AfB6115b954Bd326cbe7B4BA576818f6"
USDT_USD_chainlik = "0x3E7d1eAB13ad0104d2750B8863b489D65364e32D"
GHO_USD_chainlink = "0x3f12643D3f6f874d39C2a4c9f2Cd6f2DbAC877FC"
crvUSD_USD_chainlink = "0xEEf0C605546958c1f899b6fB336C20671f9cD49F"
USDs_USD_chainlink = "0xfF30586cD0F29eD462364C7e81375FC0C71219b1"  # Fomerly DAI


@dataclass
class StableCoinConsants:
    token_address: str
    symbol: str
    decimals: int
    backing_call: Call = None
    safe_price_call: Call = None
    spot_price_calls: list[Call] = None


def constant_1(*args):
    return 1.0


def make_dummy_1_call(name: str) -> Call:
    # Dummy call that always returns 1.0
    return Call(
        "0x0000000000000000000000000000000000000000",
        [
            "dummy()(uint256)",
        ],
        [(name, constant_1)],
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


DAI_to_USDC_spot_price = Call(
    "0xbEbc44782C7dB0a1A60Cb6fe97d0b483032FF1C7",
    ["get_dy(int128,int128,uint256)(uint256)", 0, 1, int(1e18)],
    [("DAI_to_USDC_spot_price", safe_normalize_6_with_bool_success)],
)

DAI_to_USDC_spot_price2 = Call(
    "0xA5407eAE9Ba41422680e2e00537571bcC53efBfD",
    ["get_dy(int128,int128,uint256)(uint256)", 0, 1, int(1e18)],
    [("DAI_to_USDC_spot_price2", safe_normalize_6_with_bool_success)],
)


dai_constants = StableCoinConsants(
    token_address=DAI,
    symbol="DAI",
    decimals=18,
    backing_call=make_dummy_1_call("DAI_backing"),
    safe_price_call=make_chainlink_price_call(DAI_USD_chainlink, 18, "DAI_safe_price"),
    spot_price_calls=[DAI_to_USDC_spot_price, DAI_to_USDC_spot_price2],
)


USDT_to_USDC_spot_price = Call(
    "0xbEbc44782C7dB0a1A60Cb6fe97d0b483032FF1C7",
    ["get_dy(int128,int128,uint256)(uint256)", 2, 1, int(1e6)],
    [("USDT_to_USDC_spot_price", safe_normalize_6_with_bool_success)],
)

USDT_to_USDC_spot_price2 = Call(
    "0xA5407eAE9Ba41422680e2e00537571bcC53efBfD",
    ["get_dy(int128,int128,uint256)(uint256)", 2, 1, int(1e6)],
    [("USDT_to_USDC_spot_price2", safe_normalize_6_with_bool_success)],
)


usdt_constants = StableCoinConsants(
    token_address=USDT,
    symbol="USDT",
    decimals=6,
    backing_call=make_dummy_1_call("USDT_backing"),
    safe_price_call=make_chainlink_price_call(USDT_USD_chainlik, 6, "USDT_safe_price"),
    spot_price_calls=[USDT_to_USDC_spot_price, USDT_to_USDC_spot_price2],
)


# USDC spot price == 1 since that is the reference asset

usdc_constants = StableCoinConsants(
    token_address=USDC,
    symbol="USDC",
    decimals=6,
    backing_call=make_dummy_1_call("USDC_backing"),
    safe_price_call=make_chainlink_price_call(USDC_USD_chainlink, 6, "USDC_safe_price"),
    spot_price_calls=[make_dummy_1_call("USDC_to_USDC_spot_price"), make_dummy_1_call("USDC_to_USDC_spot_price_2")],
)


USDe_to_USDC_spot_price = Call(
    "0x02950460E2b9529D0E00284A5fA2d7bDF3fA4d72",
    ["get_dy(int128,int128,uint256)(uint256)", 0, 1, int(1e18)],
    [("USDe_to_USDC_spot_price", safe_normalize_6_with_bool_success)],
)

sUSEe_to_USDC_spot_price = build_balancer_query_swap_call(
    "0xb819feef8f0fcdc268afe14162983a69f6bf179e000000000000000000000689",
    sUSDe,
    USDC,
    int(1e18),
    "sUSEe_to_USDC_spot_price",
    6,
)

usde_constants = StableCoinConsants(
    token_address=USDe,
    symbol="USDe",
    decimals=18,
    backing_call=make_dummy_1_call("USDe_backing"),
    safe_price_call=make_chainlink_price_call(USDe_USD_chainlink, 18, "USDe_safe_price"),
    spot_price_calls=[USDe_to_USDC_spot_price, sUSEe_to_USDC_spot_price],
)


# there is not another good source of sUSD liqudity
# only 3rd party pahts
sUSD_to_USDC_spot_price = Call(
    "0xA5407eAE9Ba41422680e2e00537571bcC53efBfD",
    ["get_dy(int128,int128,uint256)(uint256)", 3, 1, int(1e18)],
    [("sUSD_to_USDC_spot_price", safe_normalize_6_with_bool_success)],
)


usds_constants = StableCoinConsants(
    token_address=USDs,
    symbol="USDs",
    decimals=18,
    backing_call=make_dummy_1_call("USDs_backing"),
    safe_price_call=make_chainlink_price_call(USDs_USD_chainlink, 18, "USDs_safe_price"),
    spot_price_calls=[
        sUSD_to_USDC_spot_price,
    ],
)


crvUSD_to_USDC_spot_price = Call(
    "0x4DEcE678ceceb27446b35C672dC7d61F30bAD69E",
    ["get_dy(int128,int128,uint256)(uint256)", 1, 0, int(1e18)],
    [("crvUSD_to_USDC_spot_price", safe_normalize_6_with_bool_success)],
)


crvUSD_to_USDT_spot_price = Call(
    "0x390f3595bCa2Df7d23783dFd126427CCeb997BF4",
    ["get_dy(int128,int128,uint256)(uint256)", 1, 0, int(1e18)],
    [("crvUSD_to_USDT_spot_price", safe_normalize_6_with_bool_success)],
)


crvUSD_constants = StableCoinConsants(
    token_address=crvUSD,
    symbol="crvUSD",
    decimals=18,
    backing_call=make_dummy_1_call("crvUSD_backing"),
    safe_price_call=make_chainlink_price_call(crvUSD_USD_chainlink, 18, "crvUSD_safe_price"),
    spot_price_calls=[crvUSD_to_USDC_spot_price, crvUSD_to_USDT_spot_price],
)


GHO_to_USDC_spot_price = build_balancer_query_swap_call(
    "0x8353157092ed8be69a9df8f95af097bbf33cb2af0000000000000000000005d9",
    GHO,
    USDC,
    int(1e18),
    "GHO_to_USDC_spot_price",
    6,
)


gho_constants = StableCoinConsants(
    token_address=GHO,
    symbol="GHO",
    decimals=18,
    backing_call=make_dummy_1_call("GHO_backing"),
    safe_price_call=make_chainlink_price_call(GHO_USD_chainlink, 18, "GHO_safe_price"),
    spot_price_calls=[GHO_to_USDC_spot_price],
)


scrvUSD_constants = StableCoinConsants(
    token_address=scrvUSD,
    symbol="scrvUSD",
    decimals=18,
    backing_call=Call(
        scrvUSD,
        [
            "pricePerShare()(uint256)",
        ],
        [("scrvUSD_backing", safe_normalize_with_bool_success)],
    ),
    safe_price_call=None,
    spot_price_calls=None,
)

sUSDe_constants = StableCoinConsants(
    token_address=sUSDe,
    symbol="sUSDe",
    decimals=18,
    backing_call=Call(
        sUSDe,
        ["convertToAssets(uint256)(uint256)", int(1e18)],
        [("sUSDe_backing", safe_normalize_with_bool_success)],
    ),
    safe_price_call=None,
    spot_price_calls=None,
)

sUSDs_constants = StableCoinConsants(
    token_address=sUSDs,
    symbol="sUSDs",
    decimals=18,
    backing_call=Call(
        sUSDs,
        ["convertToAssets(uint256)(uint256)", int(1e18)],
        [("sUSDs_backing", safe_normalize_with_bool_success)],
    ),
    safe_price_call=None,
    spot_price_calls=None,
)

stablecoin_constants = [
    dai_constants,
    usde_constants,
    usdc_constants,
    usdt_constants,
    usds_constants,
    gho_constants,
    crvUSD_constants,
    scrvUSD_constants,
    sUSDe_constants,
    sUSDs_constants,
]


# from mainnet_launch.pages.asset_discounts.stable_coin_pricing import (
#     StableCoinConsants,
#     stablecoin_constants,
#     stables,
#     usde_constants,
# )
# from mainnet_launch.data_fetching.get_state_by_block import (
#     get_raw_state_by_blocks,
#     get_state_by_one_block,
#     build_blocks_to_use,
# )
# from mainnet_launch.constants import ETH_CHAIN


# # pool_id = bytes.fromhex("0xb819feef8f0fcdc268afe14162983a69f6bf179e000000000000000000000689"[2:])
# # amount_in = int(1e18)
# # swap_kind = 0
# # user_data = b""

# how to get the
# arg1 = ( pool_id,
#         swap_kind,
#         sUSDe,
#         USDC,
#         amount_in,
#         user_data,  )
# arg2 = (
#         '0x0000000000000000000000000000000000000000',
#         from_internal_balance,
#         '0x0000000000000000000000000000000000000000',
#         to_internal_balance
# )

# call_obj = Call(
#     "0xE39B5e3B6D74016b2F6A9673D7d7493B6DF549d5",  # Balancer Queries contract address
#     [
#         # Function signature for querySwap with two tuple parameters:
#         "querySwap((bytes32,uint8,address,address,uint256,bytes),(address,bool,address,bool))(uint256)",
#         arg1, arg2


#     ],
#     [("usdc_out", safe_normalize_6_with_bool_success)]
# )

# print(get_state_by_one_block([call_obj],22027082, ETH_CHAIN ))


# # does not work, only can see very old pools
# # def make_curve_get_best_rate_call(token_address: str, token_symbol: str, decimals: int):
# #     curve_router = "0x99a58482BD75cbab83b27EC03CA68fF489b5788f"

# #     def _handle_get_best_rate_response(success, args):
# #         pool, price = args
# #         if success:
# #             return price / 1e6  # since USDC and USDT only have 6 decimals

# #     token_to_usdc_call = Call(
# #         curve_router,
# #         ["get_best_rate(address,address,uint256)((address,uint256))", token_address, USDC, int(10 ** (decimals))],
# #         [(f"{token_symbol}_spot_to_USDC", _handle_get_best_rate_response)],
# #     )

# #     token_to_usdt = Call(
# #         curve_router,
# #         ["get_best_rate(address,address,uint256)((address,uint256))", token_address, USDT, int(10 ** (decimals))],
# #         [(f"{token_symbol}_spot_to_USDT", _handle_get_best_rate_response)],
# #     )

# #     return token_to_usdc_call, token_to_usdt
