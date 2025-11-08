import pandas as pd


def compute_series_apy(end_of_day_prices_df: pd.DataFrame) -> pd.Series:
    if not isinstance(end_of_day_prices_df.index, pd.DatetimeIndex):
        raise ValueError("Index of end_of_day_prices_df must be datetime")
    end_of_day_prices_df = end_of_day_prices_df.sort_index()
    n_years = (end_of_day_prices_df.index[-1] - end_of_day_prices_df.index[0]).days / 365
    total_return = end_of_day_prices_df.iloc[-1] / end_of_day_prices_df.iloc[0] - 1
    apy = 100 * ((1 + total_return) ** (1 / n_years) - 1)
    return apy.round(2)


def compute_most_recent_30_days_apy(end_of_day_prices_df: pd.DataFrame) -> pd.Series:
    if not isinstance(end_of_day_prices_df.index, pd.DatetimeIndex):
        raise ValueError("Index of end_of_day_prices_df must be datetime")
    end_of_day_prices_df = end_of_day_prices_df.sort_index()
    most_recent_30_days = end_of_day_prices_df.index[-1] - pd.Timedelta(days=30)
    recent_prices_df = end_of_day_prices_df[end_of_day_prices_df.index >= most_recent_30_days]
    recent_apy = compute_series_apy(recent_prices_df)
    return recent_apy


def compute_yield_volatility_annualized(
    end_of_day_prices_df: pd.DataFrame,
) -> pd.Series:
    if not isinstance(end_of_day_prices_df.index, pd.DatetimeIndex):
        raise ValueError("Index of end_of_day_prices_df must be datetime")

    end_of_month_prices_df = end_of_day_prices_df.resample("ME").last()
    end_of_month_returns = end_of_month_prices_df.pct_change().dropna()
    std_monthly_net_returns = end_of_month_returns.std()
    annualized_volatility = std_monthly_net_returns * (12**0.5)
    return 100 * annualized_volatility


def compute_excess_vs_benchmark_apy(end_of_day_prices_df: pd.DataFrame, benchmark_column: str) -> pd.Series:
    apy = compute_series_apy(end_of_day_prices_df)
    excess_apy = apy - apy[benchmark_column]
    return excess_apy


def compute_30_day_excess_vs_benchmark_apy(end_of_day_prices_df: pd.DataFrame, benchmark_column: str) -> pd.Series:
    apy = compute_most_recent_30_days_apy(end_of_day_prices_df)
    excess_apy = apy - apy[benchmark_column]
    return excess_apy


def compute_information_ratio(end_of_day_prices_df: pd.DataFrame, benchmark_column: str) -> pd.Series:
    if not isinstance(end_of_day_prices_df.index, pd.DatetimeIndex):
        raise ValueError("Index of end_of_day_prices_df must be datetime")

    end_of_month_prices_df = end_of_day_prices_df.resample("ME").last()
    end_of_month_returns = (100 * end_of_month_prices_df.pct_change()) * (12**0.05)
    benchmark_returns = end_of_month_returns[benchmark_column]
    monthly_excess_returns = end_of_month_returns.sub(benchmark_returns, axis=0)

    average_monthly_excess_returns = monthly_excess_returns.mean(axis=0)
    std_monthly_excess_returns = monthly_excess_returns.std(axis=0)

    information_ratio = average_monthly_excess_returns / std_monthly_excess_returns
    return information_ratio
