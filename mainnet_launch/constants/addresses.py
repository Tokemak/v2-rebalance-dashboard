from .models import TokemakAddress


DEAD_ADDRESS = "0x000000000000000000000000000000000000dEaD"
ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"

SYSTEM_REGISTRY = TokemakAddress(
    eth="0x2218F90A98b0C070676f249EF44834686dAa4285",
    base="0x18Dc926095A7A007C01Ef836683Fdef4c4371b4e",
    sonic="0x1a912EB51D3cF8364eBAEE5A982cA37f25aD8848",
    arb="0xBFd8E6C9bF2CD5466f5651746f8E946A6C7b4220",
    plasma="0x9065C0E33Bc8FB31A21874f399985e39bC187D48",
    linea="0x25f26Ec2e764c63F8D191dFE7f88c6646ca9F980",
    name="system_registry",
)

AUTOPOOL_REGISTRY = TokemakAddress(
    eth="0x7E5828a3A6Ae75426d739E798140513A2E2964E4",
    base="0x4fE7916A10B15DADEFc59D06AC81757112b1feCE",
    sonic="0x63E8e5aeBcC8C77BD4411aba375FcBDd9ce8C253",
    arb="0xc3b8F578c25bE230A2C0f56Cb466e7B8c6c9D268",
    plasma="0x0dA0E8f8dF8b6541affB071C6e0FF6835154e1dc",
    linea="0xf25f616CCc086ddA1129323381EfA1edC8d5F42c",
    name="autopool_registry",
)

ROOT_PRICE_ORACLE = TokemakAddress(
    eth="0x61F8BE7FD721e80C0249829eaE6f0DAf21bc2CaC",
    base="0xBCf67d1d643C53E9C2f84aCBd830A5EDC2661795",
    sonic="0x356d6e38efd2f33B162eC63534B449B96846751F",
    arb="0xe84CEa5553CC9D65166A7850DAB2E7712072D97F",
    plasma="0xf25BDd81822aB430F6637Ea31D8b5aDd0B6d124F",
    linea="0x03DC051eb7fe444CEBCC2e870eba4464D8175618",
    name="root_price_oracle",
)

LENS_CONTRACT = TokemakAddress(
    eth="0x146b5564dd061D648275e4Bd3569b8c285783882",
    base="0xaF05c205444c5884F53492500Bed22A8f617Aa9C",
    sonic="0xCB7E450c32D21Eb0168466c8022Ae32EF785a163",
    arb="0x590A31453390A1bB266672156A87eFB1302FC754",
    plasma="0x8dBaD46D468d57fdd1FCbA0452C8cD4d7FaE72E8",
    linea="0x92537a95b45AB695ab3EbabFc1A3c3E27AF7973c",
    name="lens_contract",
)

DESTINATION_VAULT_REGISTRY = TokemakAddress(
    eth="0x3AaC1CE01127593CA0c7f87b1Aedb1E153e152aE",
    base="0xBBBB6E844EEd5952B44C2063670093E27E21735f",
    sonic="0x005B5DD2182F4ADf9fCA299e762029337FF79fA8",
    arb="0x8d75A2b774277370d9dC8c034f23003B29032B4B",
    plasma="0x8Ccd47869E0EeA55Ba4AF520571A9C6Ce300347d",
    linea="0xc7B0617573A65cDAC06FAfD106Cf9f8503D65Da2",
    name="destination_vault_registry",
)

INCENTIVE_PRICING_STATS = TokemakAddress(
    eth="0x8607bA6540AF378cbA64F4E3497FBb2d1385f862",
    base="0xF28213d5cbc9f4cfB371599D25E232978848090d",
    sonic=DEAD_ADDRESS,
    arb=DEAD_ADDRESS,
    plasma=DEAD_ADDRESS,  # does not exist, double check
    linea=DEAD_ADDRESS,  # does not exist (double check)
    name="incentive_pricing_stats",
)

LIQUIDATION_ROW = TokemakAddress(
    eth="0xBf58810BB1946429830C1f12205331608c470ff5",
    base="0xE2F00bbC3E5ddeCfBD95e618CE36b49F38881d4f",
    sonic="0xf3b137219325466004AEb91CAa0A0Bdd2A8afc8e",
    arb="0x610Ffeb00B8312B0540DED300c683227CB3E3AB5",
    plasma="0xD3132ce50e7471cC6130Ac5Aa553149dc3B2A018",
    linea="0xc332386610bD4d555c762d7f88c17ACf96f05b3C",
    name="liquidation_row",
)

LIQUIDATION_ROW2 = TokemakAddress(
    eth="0xF570EA70106B8e109222297f9a90dA477658d481",
    base="0x7571dE594A92379c0A053E2A5004514057c10B5D",
    sonic=DEAD_ADDRESS,  # there is only one sonic liquidation row
    arb=DEAD_ADDRESS,  # only one arb liquidation row
    plasma=DEAD_ADDRESS,  # only one plasma liquidation row
    linea=DEAD_ADDRESS,  # only one linea liquidation row
    name="liquidation_row2",
)

SOLVER_ROOT_ORACLE = TokemakAddress(
    eth="0xdB8747a396D75D576Dc7a10bb6c8F02F4a3C20f1",
    base="0x67D29b2d1b422922406d6d5fb7846aE99c282de1",
    sonic="0x4137b35266A4f42ad8B4ae21F14D0289861cc970",
    arb="0x5EE5D04942DC4C78cE27c249fDacB24Aa39cBD14",
    plasma="0x03fAD8445b30bF639c5f54e9502e43BA5f4D6caD",
    linea="0x24127aaD4FB9E7d52803fa6860B9964537127E00",  # might not be right
    name="SolverRootOracle",
)

# only autoLRT on mainnet uses points
POINTS_HOOK = TokemakAddress(
    eth="0xA386067eB5F7Dc9b731fe1130745b0FB00c615C3",
    base=DEAD_ADDRESS,
    sonic=DEAD_ADDRESS,
    arb=DEAD_ADDRESS,
    plasma=DEAD_ADDRESS,
    linea=DEAD_ADDRESS,
    name="points_hook",
)

STATS_CALCULATOR_REGISTRY = TokemakAddress(
    eth="0xaE6b250841fA7520AF843c776aA58E23060E2124",
    base="0x22dd2189728B40409476F4F80CA8f2f6BdB217D2",
    sonic=DEAD_ADDRESS,
    arb=DEAD_ADDRESS,
    plasma=DEAD_ADDRESS,
    linea=DEAD_ADDRESS,
    name="stats_calculator_registry",
)


## BASE ASSETS


WETH = TokemakAddress(
    eth="0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
    base="0x4200000000000000000000000000000000000006",
    sonic="0x50c42dEAcD8Fc9773493ED674b675bE577f2634b",
    arb="0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
    plasma="0x9895D81bB462A195b4922ED7De0e3ACD007c32CB",  # we shouldn't care about this one
    linea="0xe5D7C2a44FfDDf6b295A15c148167daaAf5Cf34f",
    name="WETH",
)

USDC = TokemakAddress(
    eth="0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
    base="0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
    sonic="0x29219dd400f2Bf60E5a23d13Be72B486D4038894",
    arb="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
    plasma=DEAD_ADDRESS,
    linea="0x176211869cA2b568f2A7D4EE941E073a821EE1ff",
    name="USDC",
)

USDT = TokemakAddress(
    eth="0xdAC17F958D2ee523a2206206994597C13D831ec7",
    base="0xfde4C96c8593536E31F229EA8f37b2ADa2699bb2",
    sonic=DEAD_ADDRESS,
    arb=DEAD_ADDRESS,
    plasma="0xB8CE59FC3717ada4C02eaDF9682A9e934F625ebb",
    linea=DEAD_ADDRESS,
    name="USDT",
)

DOLA = TokemakAddress(
    eth="0x865377367054516e17014CcdED1e7d814EDC9ce4",
    base="0x4621b7A9c75199271F773Ebd9A499dbd165c3191",
    sonic=DEAD_ADDRESS,
    arb="0x6A7661795C374c0bFC635934efAddFf3A7Ee23b6",
    plasma=DEAD_ADDRESS,  # don't care about DOLA on plasma
    linea=DEAD_ADDRESS,  # don't care about DOLA on linea
    name="DOLA",
)

EURC = TokemakAddress(
    eth=DEAD_ADDRESS,
    base="0x60a3E35Cc302bFA44Cb288Bc5a4F316Fdb1adb42",
    sonic=DEAD_ADDRESS,
    arb=DEAD_ADDRESS,  # there is a EURC on arbitrum, but not certain which one to use
    plasma=DEAD_ADDRESS,  # don't care about EURC on plasma
    linea=DEAD_ADDRESS,  # don't care about EURC on linea
    name="EURC",
)


ALL_BASE_ASSETS: list[TokemakAddress] = [
    WETH,
    USDC,
    DOLA,
    EURC,
]
