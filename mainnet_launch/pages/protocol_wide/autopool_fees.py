import pandas as pd
import plotly.express as px
import streamlit as st

from mainnet_launch.database.schema.full import AutopoolFees, Blocks
from mainnet_launch.constants import ALL_AUTOPOOLS, SessionState
from mainnet_launch.database.postgres_operations import get_full_table_as_df_with_tx_hash

# TODO once we start charging fees, include plots for the USD value of shares when minted


def _load_fees_df() -> pd.DataFrame:
    if st.session_state.get(SessionState.RECENT_START_DATE):
        where_clause = Blocks.datetime >= st.session_state[SessionState.RECENT_START_DATE]

    else:
        where_clause = None

    df = get_full_table_as_df_with_tx_hash(AutopoolFees, where_clause=where_clause)
    autopool_name_map = {a.autopool_eth_addr: a.name for a in ALL_AUTOPOOLS}
    df["autopool_name"] = df["autopool_vault_address"].map(autopool_name_map).fillna(df["autopool_vault_address"])
    return df


def fetch_and_render_autopool_fees():
    st.title("Autopool Fees — Daily/Weekly Minted Shares")

    df = _load_fees_df()

    freq_label = st.radio(
        "Aggregation",
        options=["Daily", "Weekly"],
        index=0,
        horizontal=True,
    )
    freq = "1D" if freq_label == "Daily" else "7D"

    agg = (
        df.groupby("autopool_name")
        .resample(freq)["minted_shares"]
        .sum()
        .reset_index()
        .pivot(index="datetime", columns="autopool_name", values="minted_shares")
        .fillna(0.0)
    )

    fig = px.bar(agg)
    fig.update_layout(
        xaxis_title=None,
        yaxis_title="Minted Shares (sum)",
        legend_title="Autopool",
        margin=dict(l=10, r=10, t=40, b=10),
    )
    st.plotly_chart(fig, use_container_width=True)

    csv_bytes = agg.reset_index().to_csv(index=False).encode("utf-8")
    st.download_button(
        label=f"Download {freq_label.lower()} CSV",
        data=csv_bytes,
        file_name=f"autopool_fees_{freq_label.lower()}.csv",
        mime="text/csv",
    )

    with st.expander("Show raw fee rows"):
        st.dataframe(
            df.reset_index(),
            use_container_width=True,
            hide_index=True,
        )


if __name__ == "__main__":
    fetch_and_render_autopool_fees()
