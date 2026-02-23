# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Streamlit dashboard for monitoring Tokemak V2 Autopool protocols. Tracks rebalance events, NAV/TVL, returns, destination diagnostics, risk metrics, and more across multiple chains (ETH, Base, Sonic, Arbitrum, Plasma, Linea).

## Common Commands

```bash
poetry install                              # Install dependencies (requires Python 3.10)
poetry run app                              # Run main Streamlit dashboard
poetry run pytest                           # Run tests (90-day window, parallel, ~47s)
poetry run pytest --only-render-recent-data=false  # Run tests with all-time data (~72s)
poetry run pytest -k "test_name"            # Run a single test
poetry run black .                          # Format code
poetry run black --diff --check .           # Check formatting (CI lint step)
poetry run update-prod-db                   # Update production database (threaded)
poetry run slow-update-prod-db              # Update production database (sequential, ~6min)
```

**CI runs**: lint check, then pytest with 90-day window, then pytest with all-time data.

## Code Style

- **Python 3.10 strictly** — do not use 3.11+ features (no `match` statements, no `ExceptionGroup`, no `tomllib`, etc.)

## Architecture

### Data Flow

On-chain data (via Web3/Multicall) and API data (Alchemy, CoinGecko, Etherscan, etc.) → PostgreSQL database → Pandas DataFrames → Plotly charts rendered in Streamlit.

### Key Modules

- **`constants/`** — Chain definitions (`ChainData`), autopool configs (`AutopoolConstants`), contract addresses (`TokemakAddress`), and session state keys. Import pools and chains from `mainnet_launch.constants` (e.g., `AUTO_ETH`, `ETH_CHAIN`, `ALL_AUTOPOOLS`).
- **`database/`** — SQLAlchemy ORM models in `schema/full.py`. Query data with `merge_tables_as_df()` using `TableSelector` objects for type-safe multi-table JOINs returning DataFrames. See `mainnet_launch/database/postgres_operations.py` for the full API.
- **`data_fetching/`** — On-chain state via `get_state_by_one_block()` with multicall `Call` objects. Async HTTP utilities with rate limiting for third-party APIs.
- **`pages/`** — Three categories: `autopool/` (per-pool diagnostics), `risk_metrics/` (cross-pool risk analysis), `protocol_wide/` (fees, gas). Each subfolder is a tab. Page functions registered in `page_functions.py` via dictionaries (`AUTOPOOL_CONTENT_FUNCTIONS`, `RISK_METRICS_FUNCTIONS`, `PROTOCOL_CONTENT_FUNCTIONS`).
- **`slack_messages/`** — Slack notification modules for alerts (solver, depeg, concentration, incentives).

### Page Function Pattern

All page rendering functions follow `fetch_and_render_*` naming and **MUST accept exactly one argument** (typically `AutopoolConstants`) to support parametrized testing. Risk metrics pages take `(chain, base_asset, autopools)` instead. See `mainnet_launch/pages/autopool/key_metrics/nav.py` for a canonical example.

### Session State

Streamlit session state controls data windowing. `SessionState.RECENT_START_DATE` restricts queries to recent 90 days when set (used in tests and sidebar toggle). This affects queries globally.

## Gotchas

- Destinations are stitched across contract upgrades — multiple contract versions map to one logical destination in the UI. Never assume a 1:1 mapping between contracts and destinations.
- `SessionState.RECENT_START_DATE` affects all data queries globally; tests clear it via the autouse fixture in `conftest.py`.

## Testing

Tests parametrically render every page for every autopool to verify no runtime errors. The `conftest.py` autouse fixture manages session state clearing and the 90-day data window.

- `tests/test_app_pages.py` — All dashboard pages (autopool, protocol, risk metrics)
- `tests/test_marketing_pages.py` — Marketing pages (`@pytest.mark.marketing`, excluded by default)
- `tests/test_slack_messages.py` — Slack message generation

New pages MUST be registered in the appropriate `*_CONTENT_FUNCTIONS` dictionary or they will not be tested.

## Environment

See `.env_example` for required environment variables. Copy to `.env` and populate all values.
