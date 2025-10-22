import asyncio
import aiohttp
import concurrent.futures

from aiolimiter import AsyncLimiter
from aiohttp.client_exceptions import (
    ClientResponseError,
    ClientConnectionError,
    ServerDisconnectedError,
    ClientOSError,
)

from tqdm import tqdm
import pandas as pd


THIRD_PARTY_SUCCESS_KEY = "3rd_party_response_success"


class ThirdPartyAPIError(Exception):
    def __init__(self, message: str, data: dict):
        super().__init__(message)
        self.data = data


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

    def _runner(c):
        return asyncio.run(c)

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as exe:
        future = exe.submit(_runner, coro)
        return future.result()


async def _get_json_with_retry(
    session: aiohttp.ClientSession,
    rate_limiter: AsyncLimiter,
    request_kwargs: dict,
    custom_failure_function=None,
):
    while True:
        async with rate_limiter:
            try:
                try:
                    async with session.request(**request_kwargs, timeout=120) as resp:
                        if resp.status == 429:
                            print("Rate limit exceeded, retrying...")
                            await asyncio.sleep(60)
                            continue

                        try:
                            resp.raise_for_status()
                            data = await resp.json()
                            if custom_failure_function is not None and custom_failure_function(data):
                                raise ClientResponseError(
                                    request_info=resp.request_info,
                                    history=resp.history,
                                    status=resp.status,
                                    message="Custom failure function triggered",
                                    headers=resp.headers,
                                )
                            data[THIRD_PARTY_SUCCESS_KEY] = True
                            data["request_kwargs"] = request_kwargs
                            return data

                        except ClientResponseError:
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

                except (
                    ServerDisconnectedError,
                    ClientConnectionError,
                    ClientOSError,
                    asyncio.TimeoutError,
                ) as e:
                    return {
                        "url": request_kwargs.get("url"),
                        "status": None,
                        "headers": {},
                        "body": f"{type(e).__name__}: {e}",
                        THIRD_PARTY_SUCCESS_KEY: False,
                        "request_kwargs": request_kwargs,
                    }

            except Exception as e:
                return {
                    "url": request_kwargs.get("url"),
                    "status": None,
                    "headers": {},
                    "body": f"{type(e).__name__}: {e}",
                    THIRD_PARTY_SUCCESS_KEY: False,
                    "request_kwargs": request_kwargs,
                }


async def _make_many_requests_async(
    rate_limiter: AsyncLimiter, requests_kwargs: list[dict], custom_failure_function=None
):
    if not requests_kwargs:
        return []

    async with aiohttp.ClientSession() as session:

        async def runner(i: int, req_kwargs: dict):
            res: dict = await _get_json_with_retry(
                session, rate_limiter, request_kwargs=req_kwargs, custom_failure_function=custom_failure_function
            )
            res["datetime_received"] = pd.Timestamp.now(tz="UTC")
            return i, res

        tasks = [asyncio.create_task(runner(i, req)) for i, req in enumerate(requests_kwargs)]

        results: list[dict] = [None] * len(tasks)
        desc = f"Fetching 3rd-party data from {requests_kwargs[0].get('url', '')}"

        for fut in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc=desc):
            i, res = await fut
            results[i] = res

        return results


async def _make_many_requests_async_old(rate_limiter: AsyncLimiter, requests_kwargs: list[dict]):
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
    rate_limit_max_rate: int,
    rate_limit_time_period: int,
    requests_kwargs: list[dict],
) -> list[dict]:
    """Returns the values from all requests, in the same order as the input list."""
    _rate_limiter = AsyncLimiter(max_rate=rate_limit_max_rate, time_period=rate_limit_time_period)
    return _run_async_safely(_make_many_requests_async(_rate_limiter, requests_kwargs))


def make_single_request_to_3rd_party(request_kwargs: dict, custom_failure_function=None) -> dict:
    """
    Sync helper to fetch a single response.

    ```data = make_single_request_to_3rd_party({"method": "GET", "url": url, "params": params, "headers": headers})```

    custom_failure_function(await resp.json()) -> bool, should return True if the response is considered a failure.
    for APIs that return 200 OK even on errors.

    """
    _rate_limiter = AsyncLimiter(max_rate=1, time_period=1)
    return _run_async_safely(_make_many_requests_async(_rate_limiter, [request_kwargs], custom_failure_function))[0]
