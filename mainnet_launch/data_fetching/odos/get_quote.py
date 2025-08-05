from dataclasses import dataclass
import asyncio
import aiohttp
from typing import Optional, Sequence, Iterable
import pandas as pd
import concurrent.futures


# add a configurable parameter for what level of exposrue to exclude

# eg we can still trade witha pool where we ahve 5% of it


# start with a 10% threshold
# do this later


ODOS_BASE_URL = "https://api.odos.xyz"


@dataclass(frozen=True)
class QuoteNeeded:
    chain_id: int
    start_token: str  # input token address
    end_token: str  # output token address
    start_amount_fixed: str  # raw amount in base units (string)
    excluded_pool_ids: Sequence[str] = ()  # pools to blacklist for this quote


def run_async_safely(coro):
    """
    Sync wrapper around any coroutine. Works whether or not an event loop is already running.
    If there's no running loop: uses asyncio.run.
    If there is one: runs the coroutine in a separate thread's new loop and blocks for the result.
    """
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        # no running loop
        return asyncio.run(coro)

    # if we get here, there is a running loop; run in separate thread to avoid reentrancy issues
    def _runner(c):
        return asyncio.run(c)  # safe: new loop inside thread

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as exe:
        future = exe.submit(_runner, coro)
        return future.result()


# assumes run_async_safely is in scope from your existing code
ODOS_BASE_URL = "https://api.odos.xyz"


@dataclass(frozen=True)
class QuoteNeeded:
    chain_id: int
    start_token: str  # input token address
    end_token: str  # output token address
    start_amount_fixed: str  # raw amount in base units (string)
    excluded_pool_ids: Sequence[str] = ()  # pools to blacklist for this quote


async def _get_odos_quote_raw_async(
    chain_id: int,
    token_in: str,
    token_out: str,
    amount_in_fixed: str,
    excluded_pool_ids: Sequence[str],
    slippage_limit_percent: float = 0.3,
    user_addr: Optional[str] = None,
    gas_price: Optional[float] = None,
    simple: bool = False,
) -> dict:
    payload: dict = {
        "chainId": chain_id,
        "inputTokens": [{"tokenAddress": token_in, "amount": amount_in_fixed}],
        "outputTokens": [{"tokenAddress": token_out, "proportion": 1.0}],
        "slippageLimitPercent": slippage_limit_percent,
        "compact": True,
        "simple": simple,
        "poolBlacklist": list(excluded_pool_ids),
    }
    if user_addr:
        payload["userAddr"] = user_addr
    if gas_price is not None:
        payload["gasPrice"] = gas_price

    url = f"{ODOS_BASE_URL}/sor/quote/v2"
    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=payload, timeout=30) as resp:
            resp.raise_for_status()
            return await resp.json()


def flatten_odos_quote_strict_minimal(quote: dict) -> dict:
    out = {}
    out["traceId"] = quote["traceId"]
    out["blockNumber"] = quote["blockNumber"]
    out["gasEstimate"] = quote["gasEstimate"]
    out["dataGasEstimate"] = quote["dataGasEstimate"]
    out["gweiPerGas"] = quote["gweiPerGas"]
    out["gasEstimateValue"] = quote["gasEstimateValue"]
    out["netOutValue"] = quote["netOutValue"]
    out["priceImpact"] = quote["priceImpact"]
    out["percentDiff"] = quote["percentDiff"]
    out["partnerFeePercent"] = quote["partnerFeePercent"]
    out["pathId"] = quote["pathId"]

    out["inToken"] = quote["inTokens"][0]
    out["outToken"] = quote["outTokens"][0]
    out["inAmount"] = quote["inAmounts"][0]
    out["outAmount"] = quote["outAmounts"][0]
    out["inValue"] = quote["inValues"][0]
    out["outValue"] = quote["outValues"][0]
    return out


async def _batch_get_flat_quotes_async(
    quotes_needed: Iterable[QuoteNeeded],
    slippage_limit_percent: float = 0.3,
    user_addr: Optional[str] = None,
    gas_price: Optional[float] = None,
    simple: bool = False,
    concurrency: int = 5,
) -> list[dict]:
    sem = asyncio.Semaphore(concurrency)

    async def worker(qn: QuoteNeeded):
        async with sem:
            raw = await _get_odos_quote_raw_async(
                chain_id=qn.chain_id,
                token_in=qn.start_token,
                token_out=qn.end_token,
                amount_in_fixed=qn.start_amount_fixed,
                excluded_pool_ids=qn.excluded_pool_ids,
                slippage_limit_percent=slippage_limit_percent,
                user_addr=user_addr,
                gas_price=gas_price,
                simple=simple,
            )
            flat = flatten_odos_quote_strict_minimal(raw)
            # attach original request context
            flat["requested_start_token"] = qn.start_token
            flat["requested_end_token"] = qn.end_token
            flat["requested_amount"] = qn.start_amount_fixed
            flat["excluded_pool_ids"] = ",".join(qn.excluded_pool_ids)
            flat["chain_id"] = qn.chain_id
            return flat

    tasks = [worker(qn) for qn in quotes_needed]
    results = await asyncio.gather(*tasks, return_exceptions=False)
    return results


def batch_get_flat_quotes(
    quotes_needed: Iterable[QuoteNeeded],
    slippage_limit_percent: float = 0.3,
    user_addr: Optional[str] = None,
    gas_price: Optional[float] = None,
    simple: bool = False,
    concurrency: int = 5,
) -> pd.DataFrame:
    coro = _batch_get_flat_quotes_async(
        quotes_needed=quotes_needed,
        slippage_limit_percent=slippage_limit_percent,
        user_addr=user_addr,
        gas_price=gas_price,
        simple=simple,
        concurrency=concurrency,
    )
    flat_list = run_async_safely(coro)
    return pd.DataFrame(flat_list)


# Example usage
if __name__ == "__main__":
    q1 = QuoteNeeded(
        chain_id=1,
        start_token="0x9D39A5DE30e57443BfF2A8307A4256c8797A3497",  # sUSDe
        end_token="0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",  # USDC
        start_amount_fixed="100000000000000000000",  # 100 units
        excluded_pool_ids=["0xPoolToExclude1", "0xPoolToExclude2"],
    )
    q2 = QuoteNeeded(
        chain_id=1,
        start_token="0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",  # WETH
        end_token="0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",  # USDC
        start_amount_fixed="500000000000000000",  # 0.5 WETH
        excluded_pool_ids=[],  # no exclusions
    )

    df = batch_get_flat_quotes(
        [q1, q2],
        slippage_limit_percent=0.5,
        user_addr="0x000000000000000000000000000000000000dEaD",
        concurrency=2,
    )
    print(df.T)
    pass
