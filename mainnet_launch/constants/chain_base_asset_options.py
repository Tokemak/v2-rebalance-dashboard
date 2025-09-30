from .addresses import *
from .chains import *
from .autopools import *


CHAIN_BASE_ASSET_GROUPS = {
    (ETH_CHAIN, WETH): (AUTO_ETH, AUTO_LRT, BAL_ETH, DINERO_ETH, SILO_ETH),
    (ETH_CHAIN, USDC): (AUTO_USD, SILO_USD),
    (ETH_CHAIN, DOLA): (AUTO_DOLA,),
    (BASE_CHAIN, WETH): (BASE_ETH,),
    (BASE_CHAIN, USDC): (BASE_USD,),
    (BASE_CHAIN, EURC): (BASE_EUR,),
    (SONIC_CHAIN, USDC): (SONIC_USD,),
    (ARBITRUM_CHAIN, USDC): (ARB_USD,),
    (PLASMA_CHAIN, USDT): (PLASMA_USD,),
}
