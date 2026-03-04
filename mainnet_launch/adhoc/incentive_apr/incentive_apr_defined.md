# Incentive APR definition



## LP level

This lets us make claims like over window (days, 0, 1) 

1e18 lp token went from being eligable to have (30 CRV, 100CVX) -> (31CRV, 102 CVX) at the start and end of time window. 

And then some price there. 

Just a safe price of a lp token and how many could be claimed for a single lp token. removes all the TVL complexity  

We need an interface


## Destination Level

This lets us make claims like over window (days, 0, 1) this destination earned X% Incentive APR

Incentive APR depends on a window size. 

Incentive APR between days (0, 1) :=  (base asset value earned in incentives) / (time weighted average) tvl in the destination 


# TVL notes

Often the TVL in a destination will change within a window

- Generally small changes can happen from the price of the asset changing in the pool. eg underlying token price changes. 
    My suspicion is that the change in price of the underlyiing token will be dominated by people adding or withdawing tvl

- Larger changes can happen when people move economic value to or from the destinations. eg, people add or remove liqudity. 


# Rewards issues

Incentive tokens are typically done in the following fashion

Each second, there are X incentives that are allocated pro rata (per capital, read like per capita but per dollar) that can be claimed by capital providers for a destination.

- Those reward tokens change in price
- Often the per second rate changes in a step function. 

    - Often destinations change from some rewards -> 0 rewards per second because the rewards expire at the end of a week and then are not topped off
    - Often destinations change from X rewards per second -> Y rewards per second
    - Often destinations add or remove reward tokens


- I suspect that we can remove this variation of this case by putting in a fixed $1 worth of value into a destination.


## Contracts


Most staking reward contracts are either this one exactly or closely based on it 
https://github.com/Synthetixio/synthetix/blob/develop/contracts/StakingRewards.sol

- Genearlly we can query something like rewardPerTokenStored to see the delta in reward tokens earned over a period

