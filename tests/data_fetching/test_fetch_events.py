"""Makes external calls, don't run as part of regular tests."""

import pytest


def test_fetch_events_recusive_split():
    pass


def test_fetch_events_no_events():
    pass


def test_fetch_recent_events_on_each_chain():
    # weth, - 1000 blocks on sonic, eth, base,
    # expect at least some transfers
    pass


def test_fail_if_block_after_highest_block():
    pass
