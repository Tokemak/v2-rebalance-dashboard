# TVL methods




## Robust ways to get the TVL of a destination

### most robust, 
Get the tvl at every block, average

### medium robust

break into smaller windows. eg hourly window. 
Get the tvl at the end of each hour, average those. values as the average tvl


### not robust, use first or last day as average


All this needs to no depend on us having contracts

Alternative

Put 1 lp token into the pool.

Track how much more incentive tokens this one has claimed voer time. This tracks the value earned by a single lp token


readable_name = "Fluid USD Coin (fluid)"
pool = "0x9Fb7b4477576Fe5B32be4C1843aFB1e55F251B33"
destination_vault_address = "0x7876F91BB22148345b3De16af9448081E9853830"
