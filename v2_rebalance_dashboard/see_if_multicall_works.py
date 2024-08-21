from multicall import Multicall, Call
from get_state_by_block import safe_get_raw_state_by_block, eth_client


def SAFE_NORMALIZE_UINT256_WITH_BOOL_SUCCESS(success: int, value: int):
    if success:
        return int(value) / 1e18
    return None


# def build_balance_of_call(name: str, contract_address: str, wallet_address: str, attempt_fetch: bool = False) -> Call:
#     """Get a wallet balance of an ERC20 token"""
#     cleaning_function = cast_to_string_with_bool_success if attempt_fetch else cast_to_string
#     return Call(
#         contract_address,
#         ["balanceOf(address)(uint256)", wallet_address],
#         [(name, cleaning_function)],
#     )


def nav_per_share_call(name: str, autopool_vault_address: str) -> Call:
    return Call(
        autopool_vault_address,
        ["convertToAssets(uint256)(uint256)", int(1e18)],
        [(name, SAFE_NORMALIZE_UINT256_WITH_BOOL_SUCCESS)],
    )


BALANCER_AUTO_POOL = (
    "0xB86723da7d02C91b5E421Ed7883C35f732556F13"  # AUTOPOOL ETH STRATEGY
)
MAIN_AUTO_POOL = "0x6D81BB06Cf70f05B93231875D2A2848d0a5bD9f8"
balETH_auto_pool_vault = "0x72cf6d7C85FfD73F18a83989E7BA8C1c30211b73"

n = nav_per_share_call("balETH_navPerShare", balETH_auto_pool_vault)
import asyncio

df = asyncio.run(safe_get_raw_state_by_block([n], [20577831, 20577831 - 6000]))

print(df)
