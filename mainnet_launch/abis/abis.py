import json
from pathlib import Path


ABI_DIR = Path(__file__).parent.parent / "abis"

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

with open(ABI_DIR / "chainlink_keeper_registry_abi.json", "r") as fin:
    CHAINLINK_KEEPER_REGISTRY_ABI = json.load(fin)
