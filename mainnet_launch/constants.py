from dataclasses import dataclass
from time import time

def time_decorator(func):
    def wrapper(*args, **kwargs):
        start_time = time()
        result = func(*args, **kwargs)
        elapsed_time = time() - start_time
        print(f"{func.__name__} took {elapsed_time:.4f} seconds.")
        return result
    return wrapper



@dataclass
class AutopoolConstants:
    name:str
    autopool_eth_addr:str
    autopool_eth_strategy_addr:str

# mainnet as of sep 16, 2024


SYSTEM_REGISTRY = '0xB20193f43C9a7184F3cbeD9bAD59154da01488b4'
AUTOPOOL_REGISTRY = '0x7E5828a3A6Ae75426d739E798140513A2E2964E4'


BAL_ETH = AutopoolConstants('balETH', '0x6dC3ce9C57b20131347FDc9089D740DAf6eB34c5', '0xabe104560D0B390309bcF20b73Dca335457AA32e')
AUTO_ETH = AutopoolConstants('autoETH', '0x0A2b94F6871c1D7A32Fe58E1ab5e6deA2f114E56', '0xf5f6addB08c5e6091e5FdEc7326B21bEEd942235')
AUTO_LRT = AutopoolConstants('autoETH', '0xE800e3760FC20aA98c5df6A9816147f190455AF3', '0x72a726c10220280049687E58B7b05fb03d579109')

ALL_AUTOPOOLS = [BAL_ETH, AUTO_ETH, AUTO_LRT]