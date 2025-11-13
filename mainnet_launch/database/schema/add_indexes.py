"""Create indexes to optimize queries."""

from mainnet_launch.database.schema.full import Session


import sys
import logging

def sequential_test():
    from mainnet_launch.pages.autopool import AUTOPOOL_CONTENT_FUNCTIONS as _fns
    from mainnet_launch.constants import AUTO_USD
    from mainnet_launch.database.schema.full import ENGINE

    ENGINE.echo = True

    autopool = AUTO_USD
    for key, fn in _fns.items():
        fn(autopool=autopool)


if __name__ == "__main__":
    # Open file & redirect stdout/stderr
    # f = open("output.txt", "w")
    # sys.stdout = f
    # sys.stderr = f

    # # Redirect SQLAlchemy echo logs
    # sa_logger = logging.getLogger('sqlalchemy.engine')
    # sa_logger.setLevel(logging.INFO)
    # sa_logger.addHandler(logging.StreamHandler(f))

    sequential_test()

# poetry run python mainnet_launch/database/schema/add_indexes.py 2>&1 | tee output.txt


# this runs the tests with a single worker
# bench mark to test performance
# poetry run pytest -n1

# (306 durations < 0.005s hidden.  Use -vv to show these durations.)
# ================================================ 153 passed in 489.63s (0:08:09) =====



# that is a slow query 12 seconds 7 seconds, 13 seconds, can be made much faster



# SELECT
#     destination_token_values.token_address,
#     destination_token_values.destination_vault_address,
#     destination_token_values.quantity,
#     destination_token_values.block,
#     tokens.symbol,
#     token_values.safe_price,
#     token_values.denominated_in,
#     token_values.backing,
#     autopool_destination_states.owned_shares,
#     destination_states.underlying_token_total_supply,
#     destination_states.lp_token_safe_price,
#     destination_states.incentive_apr,
#     destination_states.fee_apr,
#     destination_states.base_apr,
#     destination_states.fee_plus_base_apr,
#     destination_states.total_apr_out,
#     destination_states.total_apr_in,
#     destinations.underlying_name,
#     destinations.exchange_name,
#     blocks.datetime
# FROM destination_token_values
# JOIN tokens
#   ON tokens.token_address = destination_token_values.token_address AND tokens.chain_id = destination_token_values.chain_id
# JOIN token_values
#   ON token_values.chain_id = destination_token_values.chain_id AND token_values.token_address = destination_token_values.token_address AND token_values.block = destination_token_values.block AND token_values.token_address = destination_token_values.token_address
# JOIN autopool_destination_states
#   ON destination_token_values.chain_id = autopool_destination_states.chain_id AND destination_token_values.destination_vault_address = autopool_destination_states.destination_vault_address AND destination_token_values.block = autopool_destination_states.block AND destination_token_values.chain_id = autopool_destination_states.chain_id
# JOIN destination_states
#   ON destination_states.chain_id = destination_token_values.chain_id AND destination_states.destination_vault_address = destination_token_values.destination_vault_address AND destination_states.block = destination_token_values.block
# JOIN destinations
#   ON destinations.chain_id = destination_token_values.chain_id AND destinations.destination_vault_address = destination_token_values.destination_vault_address
# JOIN blocks
#   ON blocks.chain_id = token_values.chain_id AND blocks.block = token_values.block
# WHERE
#     (autopool_destination_states.autopool_vault_address = '0xa7569A44f348d3D70d8ad5889e50F78E33d80D35' AND token_values.denominated_in = '0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48' AND destination_token_values.denominated_in = '0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48' AND tokens.chain_id = 1 AND blocks.datetime > '4-8-2025')





create_some_indexes = """

CREATE INDEX CONCURRENTLY
    idx_autopool_destination_states__autopool_vault_address__chain_id__block__destination_vault_address
ON autopool_destination_states (
    autopool_vault_address,
    chain_id,
    block,
    destination_vault_address
);

CREATE INDEX CONCURRENTLY
    idx_destination_token_values__chain_id__destination_vault_address__block__denominated_in__token_address
ON destination_token_values (
    chain_id,
    destination_vault_address,
    block,
    denominated_in,
    token_address
);


CREATE INDEX CONCURRENTLY
    idx_blocks__chain_id__datetime
ON blocks (
    chain_id,
    datetime
);

"""

# before 0.01s call     tests/test_app_pages.py::test_risk_metrics_pages[risk-Exit Liquidity Quotes-plasma-USDT]

# (306 durations < 0.005s hidden.  Use -vv to show these durations.)
# ================================================ 153 passed in 489.63s (0:08:09) ====

# 0.01s call     tests/test_app_pages.py::test_risk_metrics_pages[risk-Exit Liquidity Quotes-plasma-USDT]

# (306 durations < 0.005s hidden.  Use -vv to show these durations.)
# ================================================ 153 passed in 846.97s (0:14:06) =================================================
# (mainnet-launch-py3.10) pb@Parkers-Mac-Studio v2-rebalance-dashboard % 

# after adding the indexes it lkooks like ti got worse, by a lot

# trying agin