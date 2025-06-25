import aiohttp
import asyncio
import nest_asyncio
import pandas as pd

from mainnet_launch.constants import *
from mainnet_launch.database.schema.full import *
from mainnet_launch.database.schema.postgres_operations import *

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


async def quote_in_loop():
    autopool = SONIC_USD
    token_orms = get_full_table_as_orm(Tokens, where_clause=Tokens.chain_id == 146)
    base_sell_amount = 100
    tokens_df = pd.DataFrame.from_records([t.to_record() for t in token_orms])

    # first pass to filter out errors
    initial_df = await fetch_quote_df(token_orms, base_sell_amount, autopool)
    merged = pd.merge(initial_df, tokens_df, how="left", left_on="sellToken", right_on="token_address")
    good_addrs = merged[merged["error"].isna()]["token_address"].unique().tolist()
    quote_able_tokens_orm = [t for t in token_orms if t.token_address in good_addrs]

    all_quotes = []
    for i in range(30):
        new_df = await fetch_quote_df(quote_able_tokens_orm, 20_000, autopool)
        new_df = pd.merge(new_df, tokens_df, how="left", left_on="sellToken", right_on="token_address")
        print(new_df["symbol"].value_counts())
        all_quotes.append(new_df)

        long_df = pd.concat(all_quotes, axis=0)
        long_df.to_csv(
            "/Users/pb/Documents/Github/Tokemak/"
            "v2-rebalance-dashboard/mainnet_launch/data_fetching/"
            "quotes/quotes_records3.csv"
        )

        print("start_sleeping")
        await asyncio.sleep(60 * 2)
        print("done_sleeping")
        print(long_df.shape)


if __name__ == "__main__":
    asyncio.run(quote_in_loop())
