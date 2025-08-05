import asyncio
import aiohttp
import concurrent.futures
from aiolimiter import AsyncLimiter


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


async def _get_json_with_retry(
    session: aiohttp.ClientSession, rate_limiter: AsyncLimiter, url: str, params: dict, headers: dict
):
    while True:
        async with rate_limiter:
            async with session.get(url, params=params, headers=headers, timeout=30) as resp:
                if resp.status == 429:
                    await asyncio.sleep(60)
                    continue  # try again in 60 seconds
                resp.raise_for_status()
                data = await resp.json()
                return data


async def _make_many_requests_async(
    rate_limiter: AsyncLimiter, urls: str, params_list: list[dict], headers_list=list[dict]
):
    async with aiohttp.ClientSession() as session:
        tasks = [
            _get_json_with_retry(session, rate_limiter, url, params=params, headers=headers)
            for url, params, headers in zip(urls, params_list, headers_list)
        ]
        return await asyncio.gather(*tasks)


def make_many_get_requests_to_3rd_party(
    rate_limit_max_rate: int, rate_limit_time_period, urls: list[str], params_list: list[dict], headers_list=list[dict]
) -> list[dict]:
    rate_limiter = AsyncLimiter(max_rate=rate_limit_max_rate, time_period=rate_limit_time_period)
    return _run_async_safely(_make_many_requests_async(rate_limiter, urls, params_list, headers_list))


async def _make_single_request_async(
    url: str,
    params: dict,
    headers: dict,
) -> dict:
    """Fire one GET with retry under the limiter."""
    rate_limiter = AsyncLimiter(max_rate=1, time_period=1)
    async with aiohttp.ClientSession() as session:
        return await _get_json_with_retry(session, rate_limiter, url, params, headers)


def make_single_get_request_to_3rd_party(url: str, params: dict, headers: dict) -> dict:
    """
    Sync helper to fetch a single JSON response.
    """
    return _run_async_safely(_make_single_request_async(url, params, headers))
