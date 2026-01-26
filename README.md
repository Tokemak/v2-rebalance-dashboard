# Tokemak V2 Rebalance Dashboard

Streamlit dashboards for monitoring Tokemak V2 Autopools, protocol health, and marketing views. The project ingests onchain and offchain data, stores it in a Neon Postgres database, and renders dashboards describing past Autopool behavior for the operations and marketing teams.

## Repository layout
- `mainnet_launch/` – Python package that backs the dashboards  
  - `app/` – Streamlit entrypoints (`poetry run app`, `poetry run marketing-app`) and helpers for exporting Streamlit secrets  
  - `pages/` – Page renderers grouped into Autopool, Protocol-wide, and Risk Metrics sections  
  - `data_fetching/` – Onchain/API data collectors, multicall helpers, and rebalance plan utilities  
  - `database/` – Schema definitions, migrations, and helpers for keeping the analytics database current  
  - `constants/` – Autopool definitions, addresses, chain metadata, and shared helpers  
  - `abis/` – Contract ABIs used by the fetchers  
  - `slack_messages/` – Slack notifications used for alerts and status updates  
  - `adhoc/` – One-off analysis scripts
- `tests/` – Pytest suites that smoke-test the pages, slack notifications, and data-fetching utilities
- `.streamlit/config.toml` – Local Streamlit configuration
- `.env_example` – Template for required environment variables

## Prerequisites
- Python 3.10
- [Poetry](https://python-poetry.org/) for dependency management
- Access to the required RPC endpoints, database URLs, storage buckets, and API keys (see `.env_example`)

## Setup
1. Install dependencies:
   ```bash
   poetry install
   ```
2. Create a `.env` from `.env_example` and fill in:
   - `WHICH_ALCHEMY_URL` and the corresponding RPC URLs
   - `MAIN_DATABASE_URL`/`MAIN_READ_REPLICA_DATABASE_URL` (or other database targets)
   - Storage bucket names for each autopool
   - API keys such as `ETHERSCAN_API_KEY` and `COINGECKO_API_KEY`
   - Slack webhook/token settings for notifications
3. Generate Streamlit secrets for local runs (optional):
   ```bash
   poetry run export-config
   ```
   This writes `working_data/streamlit_config_secrets.toml` based on your `.env`.

## Running the dashboards
- Autopool & protocol diagnostics:
  ```bash
  poetry run app  # runs mainnet_launch/app/main.py via streamlit
  ```
- Marketing dashboards:
  ```bash
  poetry run marketing-app
  ```

### Useful scripts
The following Poetry scripts are available (see `pyproject.toml`):
- `update-prod-db` / `slow-update-prod-db` – keep the analytics database schema and data current
- `fetch-exit-liquidity-quotes` – pull exit liquidity quotes for held assets
- `slack-alert`, `post-daily-slack-messages`, `post-weekly-slack-messages` – Slack notifications

## Testing
Run the test suite:
```bash
poetry run pytest
```
By default, marketing and speed tests are skipped by `addopts`. To run them, clear that setting and target the marker, for example:
```bash
PYTEST_ADDOPTS="" poetry run pytest -m marketing
PYTEST_ADDOPTS="" poetry run pytest -m speed
```
