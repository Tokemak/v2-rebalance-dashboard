"""
These are historical only. 

They answer:

"what was incentive APR for this destination between these two blocks?"

"what was the base APR for this destination between these two blocks?"

# not confident on this one yet. 
"what was the apprication due to price change for this destination between these two blocks?"


"""



from abc import ABC, abstractmethod


SECONDS_PER_YEAR = 365 * 24 * 60 * 60


def get_seconds_between_blocks(start_block: int, end_block: int) -> int:
    """
    Returns the number of seconds between two blocks.
    This should be implemented based on the specific blockchain's block time.
    """
    raise NotImplementedError("Must implement get_seconds_between_blocks for the specific blockchain")


class IncentiveAprCalculator(ABC):

    @abstractmethod
    def _compute_one_lp_token_claimable_amount(self, block: int) -> dict[str, int]:
        """
        Returns the amount of incentive token claimable per 1 LP token at a given block.
        """
        pass


    def _compute_incentive_token_delta(self, start_block: int, end_block: int) -> dict[str, int]:
        """
        Computes the change in claimable incentive tokens per 1 LP token between two blocks.

        eg at the start of block we can get 0.01 CRV per LP token and at the end of block we can get 0.015 CRV per LP token, then the delta is +0.005 CRV per LP token.
        """
        start_claimable = self._compute_one_lp_token_claimable_amount(start_block) # must be in checksum strings
        end_claimable = self._compute_one_lp_token_claimable_amount(end_block)
        delta = {
            token: end_claimable.get(token, 0) - start_claimable.get(token, 0)
            for token in set(start_claimable) | set(end_claimable)
        }
        return delta


    def compute_incentive_apr(
        self, start_block: int, end_block: int, assumed_token_prices: dict[str, float], assumed_lp_token_value: float
    ) -> float:
        """
        Returns the incentive apr earnd by a destination in portion form. eg .01 for 1% APR.
        """
        incentive_token_delta = self._compute_incentive_token_delta(start_block, end_block)
        extra_incentive_token_value: float = sum(
            amount * assumed_token_prices.get(token, 0) for token, amount in incentive_token_delta.items()
        )
        seconds_between_blocks = get_seconds_between_blocks(start_block, end_block)
        portion_of_year = seconds_between_blocks / SECONDS_PER_YEAR
        incentive_apr = (extra_incentive_token_value / assumed_lp_token_value) / portion_of_year

        return incentive_apr
    

class BaseAndFeeAPRCalculator(ABC):

    @abstractmethod
    def _get_rate(self, block: int) -> float:
        """
        Returns "rate" exchange rate, virtual price, convertToAssets()

        some examples
        sUSDs.getRate() or CurvePool.get_virtual_price() or something else depending on the protocol.
        """
        pass


    def compute_base_apr(self, start_block: int, end_block: int) -> float:
        """
        Returns the base APR in portion form. eg .01 for 1% APR.
        """
        start_rate = self._get_rate(start_block)
        end_rate = self._get_rate(end_block)
        rate_delta = end_rate - start_rate

        seconds_between_blocks = get_seconds_between_blocks(start_block, end_block)
        portion_of_year = seconds_between_blocks / SECONDS_PER_YEAR
        base_apr = (rate_delta / start_rate) / portion_of_year
    
        return base_apr
    

class PriceReturnCalculator(ABC):

    @abstractmethod
    def _get_price(self, block: int) -> float:
        """
        as written this includes base in it.
        Returns the some safe, market price of 1 LP token at a given block.
        """
        pass


    def compute_price_return(self, start_block: int, end_block: int) -> float:
        """
        Returns the price return in portion form. eg .01 for 1% return.
        """
        start_price = self._get_price(start_block)
        end_price = self._get_price(end_block)
        price_return = (end_price - start_price) / start_price
    
        return price_return
