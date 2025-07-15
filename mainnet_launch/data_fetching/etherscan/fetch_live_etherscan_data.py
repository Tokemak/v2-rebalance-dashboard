# https://etherscan.io/chart/avg-txfee-usd

# https://etherscan.io/chart/gasprice

# https://etherscan.io/chart/gasused


# we want these as well for the refernce

# we are using 5B gas /M when back when we using a lot we were spending 3B gas /M

# we just got lucky on gas prices

# back of napkin we are speind .07% of all daily gas on ethereum.

# way too much


import requests
import pandas as pd
import streamlit as st
from datetime import date, timedelta
from mainnet_launch.constants import ETHERSCAN_API_KEY, ETHERSCAN_API_URL

# note can't use this, is an etherscan pro endpoint takes $200 per day


def fetch_daily_gas_prices(start_date: date) -> pd.DataFrame:
    """
    Fetches daily average gas price (in Gwei) from Etherscan

    from https://etherscan.io/chart/gasprice
    """
    params = {
        "module": "stats",
        "action": "dailyavggasprice",
        "startdate": start_date.strftime("%Y-%m-%d"),
        "enddate": date.today().strftime("%Y-%m-%d"),
        "sort": "asc",
        "apikey": ETHERSCAN_API_KEY,
        "chainid": 1,
    }
    res = requests.get(ETHERSCAN_API_URL, params=params)
    res.raise_for_status()
    data = res.json()
    pass

    # Parse into DataFrame
    df = pd.DataFrame(data["result"])
    df["date"] = pd.to_datetime(df["UTCDate"])
    df["avg_gas_price_gwei"] = df["DailyAverageGasPrice"].astype(float)

    return df


if __name__ == "__main__":
    df = fetch_daily_gas_prices(date(2025, 1, 1))

    print(df.tail())
