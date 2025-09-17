from .autopool_cumulative_volume import (
    fetch_and_render_cumulative_volume,
)

from .apr_and_tvl_by_destination_script import (
    fetch_and_render_autopool_apy_and_allocation_over_time,
)


MARKETING_PAGES_WITH_AUTOPOOL_ARG = {
    "Download APY and Allocation Data": fetch_and_render_autopool_apy_and_allocation_over_time,
}
MARKETING_PAGES_WITH_NO_ARGS = {
    "All Autopools Cumulative USD Volume": fetch_and_render_cumulative_volume,
}

__all__ = ["MARKETING_PAGES_WITH_AUTOPOOL_ARG", "MARKETING_PAGES_WITH_NO_ARGS"]
