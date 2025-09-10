import streamlit as st

from mainnet_launch.constants import AutopoolConstants


def fetch_and_render_autopool_deposits_and_withdrawals(autopool: AutopoolConstants):
    st.write(f"## {autopool.name} Autopool Deposits and Withdrawals")
