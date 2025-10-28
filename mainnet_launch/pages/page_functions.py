from mainnet_launch.app.marketing_app.marketing_pages.autopool_cumulative_volume import (
    fetch_and_render_cumulative_volume,
)

from mainnet_launch.app.marketing_app.marketing_pages.apr_and_tvl_by_destination_script import (
    fetch_and_render_autopool_apy_and_allocation_over_time,
)

# imported elsewhere
from mainnet_launch.pages.autopool import AUTOPOOL_CONTENT_FUNCTIONS
from mainnet_launch.pages.risk_metrics import RISK_METRICS_FUNCTIONS
from mainnet_launch.pages.protocol_wide import PROTOCOL_CONTENT_FUNCTIONS


MARKETING_CONTENT_FUNCTIONS = {
    "Cumulative Volume": fetch_and_render_cumulative_volume,
    "APY and Allocation Over Time": fetch_and_render_autopool_apy_and_allocation_over_time,
}
