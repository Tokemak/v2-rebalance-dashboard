from os import environ
from web3 import Web3
from pathlib import Path
import json

from dotenv import load_dotenv

load_dotenv()  # Load environment variables from .env file

import json

ALCHEMY_URL = environ["ALCHEMY_URL"]
eth_client = Web3(Web3.HTTPProvider(ALCHEMY_URL))

ROOT_PRICE_ORACLE = "0x28B7773089C56Ca506d4051F0Bc66D247c6bdb3a"
BALANCER_VAULT_ADDRESS = "0xBA12222222228d8Ba445958a75a0704d566BF2C8"

autoETH_AUTOPOOL_ETH_ADDRESS = "0x49C4719EaCc746b87703F964F09C22751F397BA0"

ROOT_PRICE_ORACLE = "0x28B7773089C56Ca506d4051F0Bc66D247c6bdb3a"
balETH_AUTOPOOL_ETH_ADDRESS = "0x72cf6d7C85FfD73F18a83989E7BA8C1c30211b73"
balETH_AUTOPOOL_ETH_STRATEGY_ADDRESS = "0xB86723da7d02C91b5E421Ed7883C35f732556F13"


ROOT_DIR = Path(__file__).parent
ABI_DIR = ROOT_DIR / "abis"


with open(ABI_DIR / "vault_abi.json", "r") as fin:
    AUTOPOOL_VAULT_ABI = json.load(fin)

with open(ABI_DIR / "strategy_abi.json", "r") as fin:
    AUTOPOOL_ETH_STRATEGY_ABI = json.load(fin)

with open(ABI_DIR / "Tokemak_BalancerAuraDestinationVault_abi.json", "r") as fin:
    BALANCER_AURA_DESTINATION_VAULT_ABI = json.load(fin)

with open(ABI_DIR / "convex_base_reward_pool_abi.json", "r") as fin:
    BASE_REWARD_POOL_ABI = json.load(fin)

with open(ABI_DIR / "convex_virtual_balance_reward_pool_abi.json", "r") as fin:
    EXTRA_REWARD_POOL_ABI = json.load(fin)

with open(ABI_DIR / "ERC_20_abi.json", "r") as fin:
    ERC_20_ABI = json.load(fin)

with open(ABI_DIR / "aura_stash_token_abi.json", "r") as fin:
    AURA_STASH_TOKEN_ABI = json.load(fin)

with open(ABI_DIR / "root_price_oracle_abi.json", "r") as fin:
    ROOT_PRICE_ORACLE_ABI = json.load(fin)
