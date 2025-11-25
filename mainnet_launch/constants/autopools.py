from .models import AutopoolConstants
from .chains import *
from .secrets import BUCKETS
from .addresses import *


AUTO_ETH = AutopoolConstants(
    "autoETH",
    "autoETH",
    autopool_eth_addr="0x0A2b94F6871c1D7A32Fe58E1ab5e6deA2f114E56",
    autopool_eth_strategy_addr="0xf5f6addB08c5e6091e5FdEc7326B21bEEd942235",
    solver_rebalance_plans_bucket=BUCKETS["AUTO_ETH"],
    chain=ETH_CHAIN,
    base_asset=WETH(ETH_CHAIN),
    block_deployed=20722908,
    data_from_rebalance_plan=False,
    base_asset_symbol="ETH",
    start_display_date="9-14-2024",
    base_asset_decimals=18,
)

BAL_ETH = AutopoolConstants(
    "balETH",
    "balETH",
    autopool_eth_addr="0x6dC3ce9C57b20131347FDc9089D740DAf6eB34c5",
    autopool_eth_strategy_addr="0xabe104560D0B390309bcF20b73Dca335457AA32e",
    solver_rebalance_plans_bucket=BUCKETS["BAL_ETH"],
    chain=ETH_CHAIN,
    base_asset=WETH(ETH_CHAIN),
    block_deployed=20722909,
    data_from_rebalance_plan=False,
    base_asset_symbol="ETH",
    start_display_date="9-14-2024",
    base_asset_decimals=18,
)

AUTO_LRT = AutopoolConstants(
    "autoLRT",
    "autoLRT",
    autopool_eth_addr="0xE800e3760FC20aA98c5df6A9816147f190455AF3",
    autopool_eth_strategy_addr="0x72a726c10220280049687E58B7b05fb03d579109",
    solver_rebalance_plans_bucket=BUCKETS["AUTO_LRT"],
    chain=ETH_CHAIN,
    base_asset=WETH(ETH_CHAIN),
    block_deployed=20722910,
    data_from_rebalance_plan=False,
    base_asset_symbol="ETH",
    start_display_date="9-14-2024",
    base_asset_decimals=18,
)

BASE_ETH = AutopoolConstants(
    "baseETH",
    "baseETH",
    autopool_eth_addr="0xAADf01DD90aE0A6Bb9Eb908294658037096E0404",
    autopool_eth_strategy_addr="0xe72a466d426F735BfeE91Db19dc509735B65b8dc",
    solver_rebalance_plans_bucket=BUCKETS["BASE_ETH"],
    chain=BASE_CHAIN,
    base_asset=WETH(BASE_CHAIN),
    block_deployed=21241103,
    data_from_rebalance_plan=False,
    base_asset_symbol="ETH",
    start_display_date="11-5-2024",
    base_asset_decimals=18,
)

DINERO_ETH = AutopoolConstants(
    "dineroETH",
    "dineroETH",
    autopool_eth_addr="0x35911af1B570E26f668905595dEd133D01CD3E5a",
    autopool_eth_strategy_addr="0x2Ade538C621A117afc4D485C79b16DD5769bC921",
    solver_rebalance_plans_bucket=BUCKETS["DINERO_ETH"],
    chain=ETH_CHAIN,
    base_asset=WETH(ETH_CHAIN),
    block_deployed=21718586,
    data_from_rebalance_plan=False,
    base_asset_symbol="ETH",
    start_display_date="2-9-2025",
    base_asset_decimals=18,
)

AUTO_USD = AutopoolConstants(
    "autoUSD",
    "autoUSD",
    autopool_eth_addr="0xa7569A44f348d3D70d8ad5889e50F78E33d80D35",
    autopool_eth_strategy_addr=None,
    solver_rebalance_plans_bucket=BUCKETS["AUTO_USD"],
    chain=ETH_CHAIN,
    base_asset=USDC(ETH_CHAIN),
    block_deployed=22032640,
    data_from_rebalance_plan=True,
    base_asset_symbol="USDC",
    start_display_date="4-8-2025",
    base_asset_decimals=6,
)


BASE_USD = AutopoolConstants(
    "baseUSD",
    "baseUSD",
    autopool_eth_addr="0x9c6864105AEC23388C89600046213a44C384c831",
    autopool_eth_strategy_addr=None,
    solver_rebalance_plans_bucket=BUCKETS["BASE_USD"],
    chain=BASE_CHAIN,
    base_asset=USDC(BASE_CHAIN),
    block_deployed=30310652,
    data_from_rebalance_plan=True,
    base_asset_symbol="USDC",
    start_display_date="5-16-2025",
    base_asset_decimals=6,
)


AUTO_DOLA = AutopoolConstants(
    "autoDOLA",
    "autoDOLA",
    autopool_eth_addr="0x79eB84B5E30Ef2481c8f00fD0Aa7aAd6Ac0AA54d",
    autopool_eth_strategy_addr=None,
    solver_rebalance_plans_bucket=BUCKETS["AUTO_DOLA"],
    chain=ETH_CHAIN,
    base_asset=DOLA(ETH_CHAIN),
    block_deployed=22582955,
    data_from_rebalance_plan=True,
    base_asset_symbol="DOLA",
    start_display_date="5-28-2025",
    base_asset_decimals=18,
)


SONIC_USD = AutopoolConstants(
    "sonicUSD",
    "sonicUSD",
    autopool_eth_addr="0xCb119265AA1195ea363D7A243aD56c73EA42Eb59",
    autopool_eth_strategy_addr=None,
    solver_rebalance_plans_bucket=BUCKETS["SONIC_USD"],
    chain=SONIC_CHAIN,
    base_asset=USDC(SONIC_CHAIN),
    block_deployed=31593624,
    data_from_rebalance_plan=True,
    base_asset_symbol="USDC",
    start_display_date="6-03-2025",  # TODO edit this date
    base_asset_decimals=6,
)


SILO_USD = AutopoolConstants(
    "siloUSD",
    "siloUSD",
    autopool_eth_addr="0x408b6A3E2Daf288864968454AAe786a2A042Df36",
    autopool_eth_strategy_addr=None,
    solver_rebalance_plans_bucket=BUCKETS["SILO_USD"],
    chain=ETH_CHAIN,
    base_asset=USDC(ETH_CHAIN),
    block_deployed=23025070,
    data_from_rebalance_plan=True,
    base_asset_symbol="USDC",
    start_display_date="8-20-2025",  # TODO move this date up
    base_asset_decimals=6,
)

SILO_ETH = AutopoolConstants(
    "siloETH",
    "siloETH",
    autopool_eth_addr="0x52F0D57Fb5D4780a37164f918746f9BD51c684a3",
    autopool_eth_strategy_addr=None,
    solver_rebalance_plans_bucket=BUCKETS["SILO_ETH"],
    chain=ETH_CHAIN,
    base_asset=WETH(ETH_CHAIN),
    block_deployed=23025073,
    data_from_rebalance_plan=True,
    base_asset_symbol="ETH",
    start_display_date="8-20-2025",  # TODO move this date up
    base_asset_decimals=18,
)

BASE_EUR = AutopoolConstants(
    "baseEUR",
    "baseEUR",
    autopool_eth_addr="0xeb042DEE6f7Ff3B45eF0A71686653D168FB02477",
    autopool_eth_strategy_addr=None,
    solver_rebalance_plans_bucket=BUCKETS["BASE_EUR"],
    chain=BASE_CHAIN,
    base_asset=EURC(BASE_CHAIN),
    block_deployed=33811934,
    data_from_rebalance_plan=True,
    base_asset_symbol="EURC",
    start_display_date="8-19-2025",  # TODO move this date up
    base_asset_decimals=6,
)


ARB_USD = AutopoolConstants(
    "arbUSD",
    "arbUSD",
    autopool_eth_addr="0xf63b7F49B4f5Dc5D0e7e583Cfd79DC64E646320c",
    autopool_eth_strategy_addr=None,
    solver_rebalance_plans_bucket=BUCKETS["ARB_USD"],
    chain=ARBITRUM_CHAIN,
    base_asset=USDC(ARBITRUM_CHAIN),
    block_deployed=377406050,
    data_from_rebalance_plan=True,
    base_asset_symbol="USDC",
    start_display_date="9-09-2025",
    base_asset_decimals=6,
)

PLASMA_USD = AutopoolConstants(
    "plasmaUSD",
    "plasmaUSD",
    autopool_eth_addr="0x4Ec8f8b0F144ce1fa280B84F01Df9e353e83EC80",
    autopool_eth_strategy_addr=None,
    solver_rebalance_plans_bucket=BUCKETS["PLASMA_USD"],
    chain=PLASMA_CHAIN,
    base_asset=USDT(PLASMA_CHAIN),
    block_deployed=1385809,
    data_from_rebalance_plan=True,
    base_asset_symbol="USDT",
    start_display_date="09-20-2025",
    base_asset_decimals=6,
)

LINEA_USD = AutopoolConstants(
    "lineaUSD",
    "lineaUSD",
    autopool_eth_addr="0xd1A6524Fccd465ECa7AF2340B3D7fd2e3bbD792a",
    autopool_eth_strategy_addr=None,
    solver_rebalance_plans_bucket=BUCKETS["LINEA_USD"],
    chain=LINEA_CHAIN,
    base_asset=USDC(LINEA_CHAIN),
    block_deployed=24833829,
    data_from_rebalance_plan=True,
    base_asset_symbol="USDC",
    start_display_date="10-23-2025",
    base_asset_decimals=6,
)


ALL_AUTOPOOLS: list[AutopoolConstants] = [
    AUTO_ETH,
    BAL_ETH,
    AUTO_LRT,
    BASE_ETH,
    DINERO_ETH,
    AUTO_USD,
    BASE_USD,
    AUTO_DOLA,
    SONIC_USD,
    SILO_USD,
    SILO_ETH,
    BASE_EUR,
    ARB_USD,
    PLASMA_USD,
    LINEA_USD,
]

ALL_AUTOPOOLS_DATA_ON_CHAIN: list[AutopoolConstants] = [AUTO_ETH, BAL_ETH, AUTO_LRT, BASE_ETH, DINERO_ETH]

ALL_AUTOPOOLS_DATA_FROM_REBALANCE_PLAN: list[AutopoolConstants] = [
    AUTO_USD,
    BASE_USD,
    AUTO_DOLA,
    SONIC_USD,
    SILO_USD,
    SILO_ETH,
    BASE_EUR,
    ARB_USD,
    PLASMA_USD,
    LINEA_USD,
]

DEPRECATED_AUTOPOOLS: list[AutopoolConstants] = [BAL_ETH, AUTO_LRT, DINERO_ETH, SILO_ETH]
