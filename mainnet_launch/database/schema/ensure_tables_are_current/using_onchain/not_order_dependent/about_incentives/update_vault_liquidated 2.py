# from dataclasses import dataclass


# @dataclass
# class VaultLiquidated:
#     """Reward tokens are sold for base asset. This at a per destiantion vault level"""

#     tx_hash: str  # primary keys
#     log_index: int  # primary keys

#     destination_vault_address: str
#     from_token_address: str
#     to_token_address: str

#     liquidated_amount: float  # in terms of to_token_address
#     liquidation_row_contract_address: str


# # https://sonicscan.org/tx/0x063b1707c4ad86daa03568bd1758dcbac73c4038d127e0342a1b11d555964622
# # this tells me when a vault was liqudiated, (what and how much)
# # we can tell what we price we got and when.
