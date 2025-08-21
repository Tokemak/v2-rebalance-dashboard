import streamlit as st
from mainnet_launch.constants.constants import *


def render_pick_chain_and_base_asset_dropdown() -> tuple[ChainData, TokemakAddress, list[AutopoolConstants]]:
    chain_base_asset_groups = {
        (ETH_CHAIN, WETH): (AUTO_ETH, AUTO_LRT, BAL_ETH, DINERO_ETH),
        (ETH_CHAIN, USDC): (AUTO_USD,),
        (ETH_CHAIN, DOLA): (AUTO_DOLA,),
        (SONIC_CHAIN, USDC): (SONIC_USD,),
        (BASE_CHAIN, WETH): (BASE_ETH,),
        (BASE_CHAIN, USDC): (BASE_USD,),
    }

    options = list(chain_base_asset_groups.keys())
    chain, base_asset = st.selectbox(
        "Pick a Chain & Base Asset:", options, format_func=lambda k: f"{k[0].name} chain and {k[1].name}"
    )
    valid_autopools = chain_base_asset_groups[(chain, base_asset)]

    return chain, base_asset, valid_autopools
