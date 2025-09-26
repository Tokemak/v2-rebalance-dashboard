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


def build_fetch_on_chain_spot_prices_function(autopool: AutopoolConstants):

    def _fetch_on_chain_spot_prices(row: pd.Series) -> dict:
        try:
            block = get_block_by_timestamp_alchemy(int(row["unix_timestamp"]), autopool.chain, closest="before")
            found_timestamp = autopool.chain.client.eth.get_block(block)

            buy_token_price_call = Call(
                ROOT_PRICE_ORACLE(autopool.chain),
                ["getPriceInQuote(address,address)(uint256)", row["buyToken"], autopool.base_asset],
                [("buy_token_price", safe_normalize_6_with_bool_success)],
            )

            sell_token_price_call = Call(
                ROOT_PRICE_ORACLE(autopool.chain),
                ["getPriceInQuote(address,address)(uint256)", row["sellToken"], autopool.base_asset],
                [("sell_token_price", safe_normalize_6_with_bool_success)],
            )

            prices = get_state_by_one_block([buy_token_price_call, sell_token_price_call], block, autopool.chain)
            row_as_dict = row.to_dict()
            row_as_dict.update(prices)
            row_as_dict["block"] = block
            row_as_dict["found_timestamp"] = found_timestamp.timestamp
            row_as_dict["prices_success"] = True

        except Exception as e:
            print("failed a row", type(e), str(e))
            row_as_dict = row.to_dict()
            row_as_dict["prices_success"] = False

        return row_as_dict

    return _fetch_on_chain_spot_prices


def main():
    bad_autopools = [BASE_EUR, SILO_ETH, SONIC_USD, BAL_ETH, DINERO_ETH, ARB_USD, SILO_USD]

    for autopool in ALL_AUTOPOOLS:
        if autopool not in bad_autopools:
            print(f"Fetching quotes for {autopool.name}")
            _fetch_on_chain_spot_prices = build_fetch_on_chain_spot_prices_function(autopool)

            autopool_save_name = WORKING_DATA_DIR / f"{autopool.name}_full_swap_matrix.csv"

            df = pd.read_csv(autopool_save_name)
            print(f"Fetching onchain prices for {len(df)} rows from {autopool_save_name}")
            df["unix_timestamp"] = df["datetime_received"].apply(lambda x: int(pd.to_datetime(x, utc=True).timestamp()))

            with ThreadPoolExecutor(max_workers=50) as executor:
                results = list(
                    tqdm(executor.map(_fetch_on_chain_spot_prices, [row for _, row in df.iterrows()]), total=len(df))
                )

            results_df = pd.DataFrame(results)
            with_spot_prices_save_name = (
                WORKING_DATA_DIR / f"swap_matrix/{autopool.name}_full_swap_matrix_with_prices.csv"
            )
            results_df.to_csv(with_spot_prices_save_name, index=False)
            print(f"Saved {len(results_df)} rows for {with_spot_prices_save_name}")


if __name__ == "__main__":
    main()
