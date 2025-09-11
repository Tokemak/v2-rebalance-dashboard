

import streamlit as st
from mainnet_launch.constants import *


def render_pick_chain_and_base_asset_dropdown() -> tuple[ChainData, TokemakAddress, tuple[AutopoolConstants]]:
    options = list(CHAIN_BASE_ASSET_GROUPS.keys())
    chain, base_asset = st.selectbox(
        "Pick a Chain & Base Asset:", options, format_func=lambda k: f"{k[0].name} chain and {k[1].name}"
    )
    valid_autopools = CHAIN_BASE_ASSET_GROUPS[(chain, base_asset)]

    return chain, base_asset, valid_autopools
