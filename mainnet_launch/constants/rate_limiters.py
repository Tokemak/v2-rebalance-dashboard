from aiolimiter import AsyncLimiter

ETHERSCAN_ASYNC_RATE_LIMITER = AsyncLimiter(max_rate=4, time_period=1)
