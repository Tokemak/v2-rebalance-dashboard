import pandas as pd

from multicall import Call
from v2_rebalance_dashboard.get_state_by_block import (
    sync_safe_get_raw_state_by_block,
    sync_get_raw_state_by_block_one_block,
    build_blocks_to_use,
    safe_normalize_with_bool_success,
)
import plotly.express as px

# move from analytics repo
destination_df = pd.read_csv(
    "/home/parker/Documents/Tokemak/v2-rebalance-dashboard/v2_rebalance_dashboard/vaults.csv", index_col=0
)[["vaultAddress", "name", "stats"]]

# struct DestinationInfo {
#     /// @notice Current underlying value at the destination vault
#     /// @dev Used for calculating totalDebt, mid point of min and max
#     uint256 cachedDebtValue;
#     /// @notice Current minimum underlying value at the destination vault
#     /// @dev Used for calculating totalDebt during withdrawal
#     uint256 cachedMinDebtValue;
#     /// @notice Current maximum underlying value at the destination vault
#     /// @dev Used for calculating totalDebt of the deposit
#     uint256 cachedMaxDebtValue;
#     /// @notice Last block timestamp this info was updated
#     uint256 lastReport;
#     /// @notice How many shares of the destination vault we owned at last report
#     uint256 ownedShares;
# }

# we need wrappers
# pool token 0 balance, token 1 balance 
# ask ahuja if he knows of a simple way to do this one, kind of complicated
# this is what we really want, but the prob is it is internal
# function calculateReserveInEthByIndex(IRootPriceOracle pricer, uint256 index) internal returns (uint256) {
#     address token = reserveTokens[index];

#     // the price oracle is always 18 decimals, so divide by the decimals of the token
#     // to ensure that we always report the value in ETH as 18 decimals
#     uint256 divisor = 10 ** CurveUtils.getDecimals(token);

#     // We are using the balances directly here which can be manipulated but these values are
#     // only used in the strategy where we do additional checks to ensure the pool
#     // is a good state
#     // slither-disable-next-line reentrancy-benign
#     return pricer.getPriceInEth(token) * IPool(poolAddress).balances(index) / divisor;
# }



def getDestinationInfo_call(name: str, autopool_vault_address: str, destination_vault: str) -> Call:

    def handle_DestinationInfo(success, DestinationInfo):
        if success:
            cachedDebtValue, cachedMinDebtValue, cachedMaxDebtValue, lastReport, ownedShares = DestinationInfo
            return {"cachedDebtValue": cachedDebtValue / 1e18, "ownedShares": ownedShares / 1e18}

    return Call(
        autopool_vault_address,
        ["getDestinationInfo(address)((uint256,uint256,uint256,uint256,uint256))", destination_vault],
        [(name, handle_DestinationInfo)],
    )



ROOT_PRICE_ORACLE = '0x28B7773089C56Ca506d4051F0Bc66D247c6bdb3a'
 
def getPriceInEth_call(name:str, token_address:str) -> Call:
    return Call(
        ROOT_PRICE_ORACLE,
        ['getPriceInEth(address)(uint256)', token_address],
        [(name, safe_normalize_with_bool_success)],
    )

# {
#         address token = reserveTokens[index];

#         // the price oracle is always 18 decimals, so divide by the decimals of the token
#         // to ensure that we always report the value in ETH as 18 decimals
#         uint256 divisor = 10 ** IERC20Metadata(token).decimals();

#         // We are using the balances directly here which can be manipulated but these values are
#         // only used in the strategy where we do additional checks to ensure the pool
#         // is a good state
#         // slither-disable-next-line reentrancy-benign,reentrancy-no-eth
#         return pricer.getPriceInEth(token) * balances[index] / divisor;
#     }


def fetch_asset_composition_over_time_to_plot():
    blocks = build_blocks_to_use()
    balETH_auto_pool_vault = "0x72cf6d7C85FfD73F18a83989E7BA8C1c30211b73"

    calls = [
        getDestinationInfo_call(name, balETH_auto_pool_vault, vault)
        for (name, vault) in zip(destination_df["name"], destination_df["vaultAddress"])
    ]
    df = sync_safe_get_raw_state_by_block(calls, blocks)


    ownedShares_df = df.map(
        lambda cell: cell["ownedShares"] if isinstance(cell, dict) and "ownedShares" in cell else 0
    )
    
    #
    
    # only look at destinatios we have touched,
    # eg where at least one of the values in eth_value_in_destination is not 0
    # this is just to make the legend cleaner
    cachedDebtValue_df = cachedDebtValue_df.loc[:, (cachedDebtValue_df != 0).any(axis=0)]

    fig = px.bar(cachedDebtValue_df)
    fig.update_layout(
        # not attached to these settings
        title="ETH Value By Destination",
        xaxis_title="Date",
        yaxis_title="ETH value",
        title_x=0.5,
        margin=dict(l=40, r=40, t=40, b=40),
        height=500,
        width=800,
    )
    return fig