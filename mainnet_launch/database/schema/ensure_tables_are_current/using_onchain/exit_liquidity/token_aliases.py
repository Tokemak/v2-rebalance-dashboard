"""

There are some tokens that are *fundementally* the same as another token, but have different addresses.

this is important because if we are trying to the the exit liqudity for a token 

we don't just care about the tokens with the same address, we care about the tokens that are the same token, but have different addresses.

For example

in order to get the sfrxETH exit liqudity:

we need to get the exit liquidity for both sfrxETH and frxETH, because they are the same token, but have different addresses.

Also

for GHO, we also need aGHO, because they are the same token, but have different addresses.

similar for scrvUSD, crvUSD 

"""

from dataclasses import dataclass
from mainnet_launch.constants.constants import *
from multicall import Call
from copy import deepcopy

from mainnet_launch.data_fetching.get_state_by_block import safe_normalize_with_bool_success, get_state_by_one_block


crvUSD = TokemakAddress(
    eth="0xf939E0A03FB07F59A73314E73794Be0E57ac1b4E",
    base="0x417Ac0e078398C154EdFadD9Ef675d30Be60Af93",
    sonic="0x7FFf4C4a827C84E32c5E175052834111B2ccd270",
    name="crvUSD",
)

scrvUSD = TokemakAddress(
    eth="0x0655977FEb2f289A4aB78af67BAB0d17aAb84367",
    base="0x646A737B9B6024e49f5908762B3fF73e65B5160c",
    sonic="0xB5f0edecFF09081354DB252CeEc000b213186fac",
    name="scrvUSD",
)


# each alias token points to a single primary token
# but a primary token can have multiple alias tokens


@dataclass
class TokenAliases:
    # the "base version" of the token
    primary_token: str

    # a ERC-4626 (style not necessarily ERC-4626 exactly) token that has a ratio
    # to convert back to the primary token
    alias_tokens: list[str]

    ratio_to_convert_to_primary_call_chain: ChainData
    ratio_to_convert_to_primary_call: Call


# it hsould find
# https://www.curve.finance/dex/base/pools/factory-stable-ng-58/deposit/
# but it is not


@dataclass
class TokenAlias:
    # the "base version" of the token
    primary_token: TokemakAddress

    # a ERC-4626 (style not necessarily ERC-4626 exactly) token that has a ratio
    # to convert back to the primary token
    alias_token: TokemakAddress

    ratio_to_convert_to_primary_call_target: TokemakAddress
    ratio_to_convert_to_primary_call: Call

    def build_call(self, chain: ChainData) -> Call:
        new_call = deepcopy(self.ratio_to_convert_to_primary_call)
        new_call.target = self.ratio_to_convert_to_primary_call_target(chain)
        print(chain.name, new_call.target)

        place_holder, return_function = new_call.returns[0]
        new_call.returns = [
            (self.alias_token(chain), return_function),
        ]
        return new_call


# on BASE 0x646A737B9B6024e49f5908762B3fF73e65B5160c
# does not have convertToAssets,

scrvUSD_and_crvUSD = TokenAlias(
    primary_token=crvUSD,
    alias_token=scrvUSD,
    ratio_to_convert_to_primary_call_target=scrvUSD,
    ratio_to_convert_to_primary_call=Call(
        DEAD_ADDRESS,
        ["convertToAssets(uint256)(uint256)", int(1e18)],
        [("place_holder", safe_normalize_with_bool_success)],
    ),
)

TOKEN_ALIASES = [
    scrvUSD_and_crvUSD,
]


def fetch_current_ratios():

    # is aave wrapped GHO the same ratio on each chain, I don't think so
    ratios = {}
    for chain in ALL_CHAINS:
        calls = [t.build_call(chain) for t in TOKEN_ALIASES]

        this_chain_ratios = get_state_by_one_block(
            calls=calls,
            block=chain.client.eth.block_number,
            chain=chain,
        )
        ratios.update(this_chain_ratios)
        print(this_chain_ratios)

    return ratios


from update_total_usd_exit_liqudity import fetch_latest_asset_exposure
from pprint import pprint

exposure_df = fetch_latest_asset_exposure()
exposure_df = exposure_df[(exposure_df["chain_id"] == 8453) & (exposure_df["reference_symbol"] == "USDC")]

current_ratios = fetch_current_ratios()

print(exposure_df.head())
pprint(current_ratios)

# what is always true
# if we have 100 scrvUSD, we can convert it to 101 crvUSD
# What we really want is
# all the exit liquidity for crvUSD, and all the exit liquidity for scrvUSD

# then to group them togethers

# so that we end up with

# crvUSD pools, usd value,
# scrvUSD pools usd value,
# as aliases, grouped together

PRIMARY_TO_ALIAS_MAPPING = {t.primary_token: t.alias_token for t in TOKEN_ALIASES}
ALIAS_TO_PRIMARY_MAPPING = {t.alias_token: t.primary_token for t in TOKEN_ALIASES}

BI_DIRECTIONAL_MAPPING = {
    **PRIMARY_TO_ALIAS_MAPPING,
    **ALIAS_TO_PRIMARY_MAPPING,
}


# it is not enough to just get the exit liquidity for crvUSD, we also need to get the exit liquidity for scrvUSD
