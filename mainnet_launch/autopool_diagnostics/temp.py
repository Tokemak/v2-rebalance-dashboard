import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.io as pio

pio.templates.default = None

from mainnet_launch.constants import AutopoolConstants, CACHE_TIME, eth_client
from mainnet_launch.constants import BAL_ETH, AUTO_ETH, AUTO_LRT
from mainnet_launch.lens_contract import get_pools_and_destinations_call, build_proxyGetDestinationSummaryStats_call
from mainnet_launch.data_fetching.get_state_by_block import get_raw_state_by_blocks, get_state_by_one_block
from mainnet_launch.destinations import DestinationDetails, get_destination_details
from mainnet_launch.data_fetching.get_state_by_block import build_blocks_to_use, identity_with_bool_success
from multicall import Call


def _handle_getPoolTokens(success, data):
    if success:
        tokens, balances, lastChangeBlock =data
        return {t:b/1e18 for t, b in zip(tokens, balances)}
            


def build_get_pool_tokens_call(name:str,
     pool_id: bytes,
) -> Call:
    """
    # note: if you try to make this call with a pool id that does not exist then the function call will thrown an error


    @dev Returns a Pool's registered tokens, the total balance for each, and the latest block when *any* of
    the tokens' `balances` changed.

    The order of the `tokens` array is the same order that will be used in `joinPool`, `exitPool`, as well as in all
    Pool hooks (where applicable). Calls to `registerTokens` and `deregisterTokens` may change this order.

    If a Pool only registers tokens once, and these are sorted in ascending order, they will be stored in the same
    order as passed to `registerTokens`.

    Total balances include both tokens held by the Vault and those withdrawn by the Pool's Asset Managers. These are
    the amounts used by joins, exits and swaps. For a detailed breakdown of token balances, use `getPoolTokenInfo`
    instead.

    """
    BALANCER_VAULT_ADDRESS = '0xBA12222222228d8Ba445958a75a0704d566BF2C8'


    return Call(
        BALANCER_VAULT_ADDRESS,
        ["getPoolTokens(bytes32)(address[],uint256[],uint256)", pool_id],
        [('tokens', identity_with_bool_success), ('balances', identity_with_bool_success)],
    )


def build_get_pool_id_call(name: str, contract_address: str) -> Call:
    return Call(contract_address, ["getPoolId()(bytes32)"], [(name, identity_with_bool_success)])


from mainnet_launch.data_fetching.get_state_by_block import get_state_by_one_block

pool_id_call = build_get_pool_id_call('pool_id', '0x88794C65550DeB6b4087B7552eCf295113794410')
pool_id = get_state_by_one_block([pool_id_call], 21041634)['pool_id']

state_call = build_get_pool_tokens_call('pxETH WETH', pool_id)
state = get_state_by_one_block([state_call], 21041634)
print(state)

pass

