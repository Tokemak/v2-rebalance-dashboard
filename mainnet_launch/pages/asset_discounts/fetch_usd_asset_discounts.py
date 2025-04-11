from multicall import Call

from mainnet_launch.data_fetching.get_state_by_block import (
    safe_normalize_6_with_bool_success,
    safe_normalize_with_bool_success,
    make_dummy_1_call,
)

# Tokemak's autoUSD specific contracts
SelfSpotEthOracle = "0x8e9A06F85A3d188f2a851d1b4fb582680727A5D7"
SolverRootOracle = "0xdB8747a396D75D576Dc7a10bb6c8F02F4a3C20f1"


DAI = "0x6B175474E89094C44Da98b954EedeAC495271d0F"
USDC = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
USDT = "0xdAC17F958D2ee523a2206206994597C13D831ec7"
GHO = "0x40D16FC0246aD3160Ccc09B8D0D3A2cD28aE6C2f"
USDS = "0xdC035D45d973E3EC169d2276DDab16f1e407384F"
USDe = "0x4c9EDD5852cd905f086C759E8383e09bff1E68B3"
FRAX = "0x853d955aCEf822Db058eb8505911ED77F175b99e"
crvUSD = "0xf939E0A03FB07F59A73314E73794Be0E57ac1b4E"
frxUSD = "0xCAcd6fd266aF91b8AeD52aCCc382b4e165586E29"

scrvUSD = "0x0655977FEb2f289A4aB78af67BAB0d17aAb84367"
sUSDS = "0xa3931d71877C0E7a3148CB7Eb4463524FEc27fbD"
sUSDe = "0x9D39A5DE30e57443BfF2A8307A4256c8797A3497"
sFRAX = "0xA663B02CF0a4b149d2aD41910CB81e23e1c41c32"
sDAI = "0x83F20F44975D03b1b09e64809B757c47f942BEeA"

sfrxUSD = "0xcf62F905562626CfcDD2261162a51fd02Fc9c5b6"


aUSDT = "0x7Bc3485026Ac48b6cf9BaF0A377477Fff5703Af8"
aUSDC = "0xD4fa2D31b7968E448877f69A96DE69f5de8cD23E"
aGHO = "0xC71Ea051a5F82c67ADcF634c36FFE6334793D24C"


stablecoin_tuples = [
    (DAI, "DAI"),
    (USDC, "USDC"),
    (USDT, "USDT"),
    (GHO, "GHO"),
    (USDS, "USDS"),
    (USDe, "USDe"),
    (FRAX, "FRAX"),
    (frxUSD, "frxUSD"),
    (crvUSD, "crvUSD"),
    (scrvUSD, "scrvUSD"),
    (sUSDS, "sUSDS"),
    (sUSDe, "sUSDe"),
    (sFRAX, "sFRAX"),
    (sfrxUSD, "sfrxUSD"),
    (sDAI, "sDAI"),
    (aUSDT, "aUSDT"),
    (aUSDC, "aUSDC"),
    (aGHO, "aGHO"),
]


def _build_autoUSD_token_backing_calls() -> list[Call]:
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

    sUSDS_backing = Call(
        sUSDS,
        ["convertToAssets(uint256)(uint256)", int(1e18)],
        [("sUSDS_backing", safe_normalize_with_bool_success)],
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

    sfrxUSD_backing = Call(
        sfrxUSD,
        ["convertToAssets(uint256)(uint256)", int(1e18)],
        [("sfrxUSD_backing", safe_normalize_with_bool_success)],
    )

    return [
        make_dummy_1_call("DAI_backing"),
        make_dummy_1_call("USDe_backing"),
        make_dummy_1_call("USDC_backing"),
        make_dummy_1_call("USDT_backing"),
        make_dummy_1_call("GHO_backing"),
        make_dummy_1_call("crvUSD_backing"),
        make_dummy_1_call("USDS_backing"),
        make_dummy_1_call("FRAX_backing"),
        make_dummy_1_call("frxUSD_backing"),
        aUSDT_backing,
        aUSDC_backing,
        aGHO_backing,
        sUSDS_backing,
        sUSDe_backing,
        scrvUSD_backing,
        sDAI_backing,
        sFRAX_backing,
        sfrxUSD_backing,
    ]


def _build_autoUSD_token_safe_price_calls() -> list[Call]:
    return [
        Call(
            SolverRootOracle,
            ["getPriceInQuote(address,address)(uint256)", addr, USDC],
            [(name + "_safe_price", safe_normalize_6_with_bool_success)],
        )
        for addr, name in stablecoin_tuples
    ]


def build_stablecoin_safe_price_and_backing_calls():
    safe_price_calls = _build_autoUSD_token_safe_price_calls()
    backing_calls = _build_autoUSD_token_backing_calls()
    return [*safe_price_calls, *backing_calls]
