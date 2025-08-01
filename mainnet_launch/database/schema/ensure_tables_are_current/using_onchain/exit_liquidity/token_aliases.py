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
