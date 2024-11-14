from mainnet_launch.constants import eth_client


def test_block_timestamp():
    assert eth_client.eth.get_block(20_000_000).timestamp == 1717281407