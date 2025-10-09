import pandas as pd
import plotly.express as px
import numpy as np
from mainnet_launch.constants import *
from mainnet_launch.database.views import get_token_details_dict


def load_swap_matrix_with_prices_data():
    swap_matrix_data2 = WORKING_DATA_DIR / "swap_matrix_prices2"
    paths = [a for a in swap_matrix_data2.glob("*.csv")]

    dfs = [pd.read_csv(p, low_memory=False) for p in paths]
    df = pd.concat(dfs).reset_index(drop=True)

    df = df[(df["3rd_party_response_success"] == True) & (df["prices_success"] == True)].copy()
    df["datetime_received"] = pd.to_datetime(df["datetime_received"], utc=True)

    df = add_actual_prices(df)

    interesting_columns = [
        "datetime_received",
        "aggregatorName",
        "autopool_name",
        "buy_symbol",
        "sell_symbol",
        "buy_amount_norm",
        "sell_amount_norm",
        "buy_amount_price",
        "buy_token_price",
        "sell_token_price",
        "label",
        "safe_value_slippage_bps",
        "long_label",
    ]
    return df.reset_index(drop=True)
    # return df[interesting_columns].reset_index(drop=True)


def add_actual_prices(df: pd.DataFrame) -> pd.DataFrame:
    token_to_decimals, token_to_symbol = get_token_details_dict()

    df["buy_amount_norm"] = df.apply(
        lambda row: int(row["buyAmount"]) / 10 ** token_to_decimals[row["buyToken"]], axis=1
    )

    df["sell_amount_norm"] = df.apply(
        lambda row: int(row["sellAmount"]) / 10 ** token_to_decimals[row["sellToken"]], axis=1
    )
    df["buy_symbol"] = df["buyToken"].map(token_to_symbol)
    df["sell_symbol"] = df["sellToken"].map(token_to_symbol)

    df["buy_amount_price"] = df.apply(lambda row: row["buy_amount_norm"] / row["sell_amount_norm"], axis=1)
    df["label"] = df["sell_symbol"] + " -> " + df["buy_symbol"]

    df["safe_value_bought"] = df.apply(lambda row: row["buy_token_price"] * row["buy_amount_norm"], axis=1)
    df["safe_value_sold"] = df.apply(lambda row: row["sell_token_price"] * row["sell_amount_norm"], axis=1)
    df["safe_value_slippage_bps"] = df.apply(
        lambda row: 1_000 * (row["safe_value_sold"] - row["safe_value_bought"]) / row["safe_value_sold"], axis=1
    )
    df["long_label"] = df["label"] + " " + df["sell_amount_norm"].astype(int).astype(str)
    return df
