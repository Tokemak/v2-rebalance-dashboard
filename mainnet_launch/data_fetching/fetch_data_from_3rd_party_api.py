import asyncio
import aiohttp
import concurrent.futures

from aiolimiter import AsyncLimiter
from tqdm import tqdm  # pip install tqdm
import pandas as pd

THIRD_PARTY_SUCCESS_KEY = "3rd_party_response_success"


def _run_async_safely(coro):
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


async def _get_json_with_retry(session: aiohttp.ClientSession, rate_limiter: AsyncLimiter, request_kwargs: dict):
    while True:
        async with rate_limiter:
            async with session.request(**request_kwargs, timeout=30) as resp:
                pass
                if resp.status == 429:
                    print("Rate limit exceeded, retrying...")
                    await asyncio.sleep(60)
                    continue

                try:
                    resp.raise_for_status()
                    # happy path
                    data = await resp.json()
                    data[THIRD_PARTY_SUCCESS_KEY] = True
                    data["request_kwargs"] = request_kwargs
                    return data

                except aiohttp.ClientResponseError:
                    # extract as much as we can from the error response
                    content_type = resp.headers.get("Content-Type", "")
                    if "application/json" in content_type:
                        body = await resp.json()
                    else:
                        body = await resp.text()

                    return {
                        "url": str(resp.url),
                        "status": resp.status,
                        "headers": dict(resp.headers),
                        "body": body,
                        THIRD_PARTY_SUCCESS_KEY: False,
                        "request_kwargs": request_kwargs,
                    }


async def _make_many_requests_async(rate_limiter: AsyncLimiter, requests_kwargs: list[dict]):
    async with aiohttp.ClientSession() as session:
        tasks = [
            _get_json_with_retry(session, rate_limiter, request_kwargs=request_kwargs)
            for request_kwargs in requests_kwargs
        ]

        results = []
        for future in tqdm(
            asyncio.as_completed(tasks),
            total=len(tasks),
            desc=f"Fetching 3rd-party data from {requests_kwargs[0]['url']}",
        ):
            res: dict = await future
            res["datetime_received"] = pd.Timestamp.now(tz="UTC")
            results.append(res)
        return results


def make_many_requests_to_3rd_party(
    rate_limit_max_rate: int, rate_limit_time_period, requests_kwargs: list[dict]
) -> list[dict]:
    rate_limiter = AsyncLimiter(max_rate=rate_limit_max_rate, time_period=rate_limit_time_period)
    return _run_async_safely(_make_many_requests_async(rate_limiter, requests_kwargs))


def make_single_request_to_3rd_party(request_kwargs: dict) -> dict:
    """
    Sync helper to fetch a single  response.
    """
    return make_many_requests_to_3rd_party(
        rate_limit_max_rate=1, rate_limit_time_period=1, requests_kwargs=[request_kwargs]
    )[0]
