import cProfile
import pstats
import io
import os
from typing import Callable, Any, Optional


DEFAULT_PACKAGE_FILTER = os.path.basename(os.getcwd())


def profile_function(
    func: Callable[..., Any], *args, package_filter: Optional[str] = DEFAULT_PACKAGE_FILTER, top_n: int = 20, **kwargs
) -> Any:
    """
    Profiles the given function call and prints the top_n functions by cumulative time.
    If package_filter is provided, only stats lines matching that substring will be shown.

    :param func: the callable to profile
    :param args: positional arguments to pass to func
    :param package_filter: substring to filter stats (e.g. "mainnet_launch")
    :param top_n: how many lines to show in the report
    :param kwargs: keyword arguments to pass to func
    :return: whatever func returns
    """
    prof = cProfile.Profile()
    prof.enable()
    result = func(*args, **kwargs)
    prof.disable()

    buf = io.StringIO()
    stats = pstats.Stats(prof, stream=buf)
    stats.sort_stats("cumtime")
    if package_filter:
        stats.print_stats(package_filter, top_n)

    print("all")
    stats.print_stats(top_n)
    print(buf.getvalue())

    return result
