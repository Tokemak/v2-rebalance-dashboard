import pandas as pd

from multicall import Call
from v2_rebalance_dashboard.get_state_by_block import (
    sync_safe_get_raw_state_by_block,
    sync_get_raw_state_by_block_one_block,
    build_blocks_to_use,
    safe_normalize_with_bool_success,
    identity_with_bool_success,    
    eth_client,
    identity_function
)
import plotly.express as px



ROOT_PRICE_ORACLE = '0x28B7773089C56Ca506d4051F0Bc66D247c6bdb3a'
BALANCER_VAULT_ADDRESS = '0xBA12222222228d8Ba445958a75a0704d566BF2C8'
balETH_auto_pool_vault = "0x72cf6d7C85FfD73F18a83989E7BA8C1c30211b73"

# { # for balancer
#         address token = reserveTokens[index];

#         // the price oracle is always 18 decimals, so divide by the decimals of the token
#         // to ensure that we always report the value in ETH as 18 decimals
#         uint256 divisor = 10 ** IERC20Metadata(token).decimals(); // 18 always

#         // We are using the balances directly here which can be manipulated but these values are
#         // only used in the strategy where we do additional checks to ensure the pool
#         // is a good state
#         // slither-disable-next-line reentrancy-benign,reentrancy-no-eth
#         return pricer.getPriceInEth(token) * balances[index] / divisor; // 18e18
#     }


def getDestinationInfo_call(name: str, autopool_vault_address: str, destination_vault: str) -> Call:

    def handle_DestinationInfo(success, DestinationInfo):
        if success:
            cachedDebtValue, cachedMinDebtValue, cachedMaxDebtValue, lastReport, ownedShares = DestinationInfo
            return {"cachedDebtValue": cachedDebtValue / 1e18, "ownedShares": ownedShares / 1e18} # can be approx, exact is not required. 

    return Call(
        autopool_vault_address,
        ["getDestinationInfo(address)((uint256,uint256,uint256,uint256,uint256))", destination_vault],
        [(name, handle_DestinationInfo)],
    )


def build_get_pool_id_call(name: str, balancer_pool_address: str) -> Call:
    return Call(balancer_pool_address, ["getPoolId()(bytes32)"], [(name, identity_with_bool_success)])


def build_getPoolTokens_call(name:str, pool_id):
    return Call(
            BALANCER_VAULT_ADDRESS,
            ["getPoolTokens(bytes32)(address[],uint256[],uint256)", pool_id],
            [(f'{name}_tokens', identity_with_bool_success), (f'{name}_balances', identity_with_bool_success), (f'{name}_last_change_block', identity_with_bool_success)]
        )


def getPriceInEth_call(name:str, token_address:str) -> Call:
    return Call(
        ROOT_PRICE_ORACLE,
        ['getPriceInEth(address)(uint256)', token_address],
        [(name, safe_normalize_with_bool_success)],
    )


def build_balancer_autopool_asset_combination_calls(blocks) -> pd.DataFrame:
    destination_df = pd.read_parquet(
    "/home/parker/Documents/Tokemak/v2-rebalance-dashboard/v2_rebalance_dashboard/vaults.parquet"
)
    destinations = Call(
            balETH_auto_pool_vault,
            ["getDestinations()(address[])"],
            [('destinations', identity_function)]
        ).__call__(_w3=eth_client)['destinations']
    
    
    destination_df =  destination_df[destination_df['vaultAddress'].str.lower().isin(destinations)].copy()
    token_name_to_address = dict()
    for token_names, token_address in zip(destination_df['token_names'],destination_df['tokens']):
        for token, address in zip(token_names, token_address):
            
            token_name_to_address[token] = address
       
    price_calls = [getPriceInEth_call(token_name, token_address) for token_name, token_address in token_name_to_address.items()]
    
    
    pool_id_calls = [build_get_pool_id_call(name, pool) for name, pool in zip(destination_df['name'], destination_df['pool'])]
    pool_ids = sync_get_raw_state_by_block_one_block(pool_id_calls, 20591987)
    
    get_pool_tokens_calls = [build_getPoolTokens_call(name, pool_ids[name]) for name in destination_df['name']] # fails
    
    
    price_df = sync_safe_get_raw_state_by_block(price_calls, blocks)
    get_pool_tokens_df = sync_safe_get_raw_state_by_block(get_pool_tokens_calls, blocks)
    # pool_id = build_get_pool_id_call('pool_id', '0x58AAdFB1Afac0ad7fca1148f3cdE6aEDF5236B6D').__call__(_w3=eth_client)['pool_id']
    # {'tokens': ('0x58aadfb1afac0ad7fca1148f3cde6aedf5236b6d', '0xa1290d69c65a6fe4df752f95823fae25cb99e5a7', '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'), 'balances': (2596148429267437984416884399779299, 4192537420423622999208, 2534823097520260013367)}
    
    
    calls = [
        getDestinationInfo_call(name, balETH_auto_pool_vault, vault)
        for (name, vault) in zip(destination_df["name"], destination_df["vaultAddress"])
    ]
    raw_ownedShares_df = sync_safe_get_raw_state_by_block(calls, blocks)
    
    ownedShares_df = raw_ownedShares_df.map(
        lambda cell: cell["ownedShares"] if isinstance(cell, dict) and "ownedShares" in cell else 0
    )
    
    return destination_df, price_df, get_pool_tokens_df, ownedShares_df
    


def fetch_asset_composition_over_time_to_plot():
    blocks = build_blocks_to_use()
    price_df, get_pool_tokens_df, ownedShares_df = build_balancer_autopool_asset_combination_calls(blocks)
    
    # we only care about when some ownedShares_df are not 0. can ignore the rest



fetch_asset_composition_over_time_to_plot()


    # #
    
    # # only look at destinatios we have touched,
    # # eg where at least one of the values in eth_value_in_destination is not 0
    # # this is just to make the legend cleaner
    # cachedDebtValue_df = cachedDebtValue_df.loc[:, (cachedDebtValue_df != 0).any(axis=0)]

    # fig = px.bar(cachedDebtValue_df)
    # fig.update_layout(
    #     # not attached to these settings
    #     title="ETH Value By Destination",
    #     xaxis_title="Date",
    #     yaxis_title="ETH value",
    #     title_x=0.5,
    #     margin=dict(l=40, r=40, t=40, b=40),
    #     height=500,
    #     width=800,
    # )
    # return fig