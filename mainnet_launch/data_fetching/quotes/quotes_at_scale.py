import aiohttp
import asyncio
import nest_asyncio
import pandas as pd

from mainnet_launch.constants import *
from mainnet_launch.database.schema.full import *
from mainnet_launch.database.schema.postgres_operations import *
import numpy as np

# allow nested loops if you ever run this inside Jupyter/debugger

nest_asyncio.apply()

_rate_limit = asyncio.Semaphore(10)
_session: aiohttp.ClientSession | None = None


async def get_session() -> aiohttp.ClientSession:
    global _session
    if _session is None or _session.closed:
        _session = aiohttp.ClientSession()
    return _session


async def fetch_swap_quote(
    chain_id: int,
    sell_token: str,
    buy_token: str,
    sell_amount: int,
    system_name: str = "gen3",
    slippage_bps: int = 50,
    include_sources: str = "",
    exclude_sources: str = "Bebop",
    sell_all: bool = True,
    timeout_ms: int = None,
    transfer_to_caller: bool = True,
) -> dict:
    async with _rate_limit:
        url = "https://swaps-pricing.tokemaklabs.com/swap-quote-v2"
        payload = {
            "chainId": chain_id,
            "systemName": system_name,
            "slippageBps": slippage_bps,
            "taker": DEAD_ADDRESS,
            "sellToken": sell_token,
            "buyToken": buy_token,
            "sellAmount": sell_amount,
            "includeSources": include_sources,
            "excludeSources": exclude_sources,
            "sellAll": sell_all,
            "timeoutMS": str(timeout_ms) if timeout_ms is not None else "",
            "transferToCaller": str(transfer_to_caller),
        }

        session = await get_session()
        async with session.post(url, json=payload) as resp:
            try:
                resp.raise_for_status()
                data = await resp.json()
                data.update(payload)
            except Exception as e:
                data = {"error": str(e), **payload}
        return data


async def fetch_quote_size_df(
    token_orms: list[Tokens], sizes: list[float], autopool: AutopoolConstants
) -> pd.DataFrame:

    tasks = []

    for size in sizes:
        for t in token_orms:

            tasks.append(
                fetch_swap_quote(
                    chain_id=t.chain_id,
                    sell_token=t.token_address,
                    buy_token=autopool.base_asset,
                    sell_amount=int(size * 10**t.decimals),
                )
            )
    quotes = await asyncio.gather(*tasks)
    quote_df = pd.DataFrame.from_records(quotes)
    return quote_df


async def fetch_quote_df(
    token_orms: list[Tokens], base_sell_amount: float, autopool: AutopoolConstants
) -> pd.DataFrame:
    # launch all requests in parallel
    quotes = await asyncio.gather(
        *(
            fetch_swap_quote(
                chain_id=t.chain_id,
                sell_token=t.token_address,
                buy_token=autopool.base_asset,
                sell_amount=int(base_sell_amount * 10**t.decimals),
            )
            for t in token_orms
        )
    )
    return pd.DataFrame.from_records(quotes)


async def quote_at_scale():
    autopool = SONIC_USD
    token_orms = get_full_table_as_orm(Tokens, where_clause=Tokens.chain_id == autopool.chain.chain_id)
    tokens_df = pd.DataFrame.from_records([t.to_record() for t in token_orms])

    # first pass to filter out errors
    initial_df = await fetch_quote_df(token_orms, 100, autopool)
    merged = pd.merge(initial_df, tokens_df, how="left", left_on="sellToken", right_on="token_address")
    tokens_we_can_get_quotes_for = merged[merged["error"].isna()]["token_address"].unique().tolist()
    quote_able_tokens_orm = [t for t in token_orms if t.token_address in tokens_we_can_get_quotes_for]
    sizes = [
        100,
        1000,
        10_000,
        50_000,
        100_000,
        200_000,
        300_000,
        400_000,
        500_000,
        1_000_000,
        1_500_000,
        2_000_000,
        2_500_000,
        3_000_000,
        4_000_000,
        5_000_000,
    ]

    scale_quote_df = await fetch_quote_size_df(quote_able_tokens_orm, sizes, autopool)
    df = pd.merge(scale_quote_df, tokens_df, how="left", left_on="sellToken", right_on="token_address")

    df["min_buy_scaled"] = df["minBuyAmount"].apply(lambda x: int(x) / 1e6 if x > 0 else np.nan)
    df["buy_scaled"] = df["buyAmount"].apply(lambda x: int(x) / 1e6 if x > 0 else np.nan)

    # scale sellAmount by its own per-row decimals
    df["sell_amount_scaled"] = df.apply(lambda row: int(row["sellAmount"]) / (10 ** row["decimals"]), axis=1)

    df["buy_ratio"] = df["buy_scaled"] / df["sell_amount_scaled"]
    df["min_buy_ratio"] = df["min_buy_scaled"] / df["sell_amount_scaled"]

    df.to_csv(
        "/Users/pb/Documents/Github/Tokemak/"
        "v2-rebalance-dashboard/mainnet_launch/data_fetching/"
        f"quotes/{autopool.name}_scale_quotes.csv"
    )

if __name__ == "__main__":
    asyncio.run(quote_at_scale())
