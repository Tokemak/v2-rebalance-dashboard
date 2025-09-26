print("a")
import pandas as pd

from mainnet_launch.constants import *
from mainnet_launch.database.schema.ensure_tables_are_current.using_onchain.helpers.update_blocks import (
    get_block_by_timestamp_defi_llama,
    get_block_by_timestamp_alchemy,
)


from mainnet_launch.data_fetching.get_state_by_block import safe_normalize_6_with_bool_success, get_state_by_one_block
from multicall import Call
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm


def _fetch_on_chain_spot_prices(row: pd.Series) -> dict:
    try:
        block = get_block_by_timestamp_alchemy(int(row["unix_timestamp"]), ETH_CHAIN, closest="before")
        found_timestamp = ETH_CHAIN.client.eth.get_block(block)

        buy_token_price_call = Call(
            ROOT_PRICE_ORACLE(ETH_CHAIN),
            ["getPriceInQuote(address,address)(uint256)", row["buyToken"], USDC(ETH_CHAIN)],
            [("buy_token_price", safe_normalize_6_with_bool_success)],
        )

        sell_token_price_call = Call(
            ROOT_PRICE_ORACLE(ETH_CHAIN),
            ["getPriceInQuote(address,address)(uint256)", row["sellToken"], USDC(ETH_CHAIN)],
            [("sell_token_price", safe_normalize_6_with_bool_success)],
        )

        prices = get_state_by_one_block([buy_token_price_call, sell_token_price_call], block, ETH_CHAIN)

        row_as_dict = row.to_dict()
        row_as_dict.update(prices)
        row_as_dict["block"] = block
        row_as_dict["found_timestamp"] = found_timestamp.timestamp
        row_as_dict["prices_success"] = True
    except Exception as e:
        print("failed a row", type(e), str(e))
        pass
        row_as_dict = row.to_dict()
        row_as_dict["prices_success"] = False

    return row_as_dict


if __name__ == "__main__":

    df = pd.read_csv(
        "mainnet_launch/database/schema/ensure_tables_are_current/using_3rd_party/combined_swap_quotes.csv"
    )
    df["unix_timestamp"] = df["datetime_received"].apply(lambda x: int(pd.to_datetime(x, utc=True).timestamp()))

    with ThreadPoolExecutor(max_workers=50) as executor:
        results = list(
            tqdm(executor.map(_fetch_on_chain_spot_prices, [row for _, row in df.iterrows()]), total=len(df))
        )

    results_df = pd.DataFrame(results)
    results_df.to_csv("swap_qutoes_with_prices.csv", index=False)
