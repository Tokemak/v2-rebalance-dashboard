# Alchemy "Blocks by Timestamp" Endpoint Deprecation Investigation

**Date:** December 17, 2025  
**Issue:** Alchemy deprecation notice for "Blocks by Timestamp" endpoint (deprecated Dec 15, 2025)  
**Status:** ✅ **No Action Required - Already Compliant**

---

## Executive Summary

This repository **does NOT use** Alchemy's deprecated "Blocks by Timestamp" endpoint. The codebase already implements the recommended alternative (Etherscan's Block Number by Timestamp API) as its primary source, with DeFi Llama as a fallback.

**No code migration is required.**

---

## Investigation Results

### Files Analyzed

The complete repository was searched for any usage of Alchemy's block-by-timestamp functionality. The analysis included:

1. Direct searches for Alchemy API patterns:
   - `getBlockByTimestamp` - No matches found
   - `alchemy.*block` patterns - No relevant matches
   - `alchemy.com` URLs - Only found token price API calls (unrelated)
   - `eth_getBlock` - No matches found

2. Block-timestamp related code:
   - Found in `mainnet_launch/database/schema/ensure_tables_are_current/using_onchain/helpers/update_blocks.py`
   - Uses Etherscan and DeFi Llama APIs, not Alchemy

### Current Implementation

#### Primary Source: Etherscan API ✅

**Function:** `get_block_by_timestamp_etherscan()`  
**Location:** `mainnet_launch/database/schema/ensure_tables_are_current/using_onchain/helpers/update_blocks.py` (Lines 119-151)

**Implementation Details:**
```python
# Uses Etherscan's getblocknobytime endpoint
API Endpoint: https://api.etherscan.io/v2/api
Parameters:
  - module: "block"
  - action: "getblocknobytime"
  - timestamp: unix_timestamp
  - closest: "before" or "after"
  - chainid: chain identifier
  - apikey: ETHERSCAN_API_KEY
```

**Citation:** This is the exact API that Alchemy recommended in their deprecation notice:
> "As an immediate replacement, Block Number by Timestamp from Etherscan offers identical functionality across all major blockchains"

**Etherscan API Documentation:** https://docs.etherscan.io/api-endpoints/blocks#get-block-number-by-timestamp

#### Fallback Source: DeFi Llama ✅

**Function:** `get_block_by_timestamp_defi_llama()`  
**Location:** `mainnet_launch/database/schema/ensure_tables_are_current/using_onchain/helpers/update_blocks.py` (Lines 154-183)

**Implementation Details:**
```python
# Uses DeFi Llama's block endpoint
API Endpoint: https://coins.llama.fi/block/{chain}/{timestamp}
Returns: Block number at or before the given timestamp
```

**DeFi Llama Documentation:** https://defillama.com/docs/api

### Usage Within Codebase

The block-by-timestamp functionality is used in two places:

1. **`update_blocks.py`** (Lines 119-183)
   - Main implementation of both functions
   - Used to find the highest block number for each day since inception
   - Function: `ensure_blocks_is_current()` (Line 82)

2. **`update_destination_states_from_rebalance_plan.py`** (Line 65)
   - Imports: `from mainnet_launch.database.schema.ensure_tables_are_current.using_onchain.helpers.update_blocks import get_block_by_timestamp_etherscan`
   - Usage: Converts rebalance plan timestamps to block numbers
   - Function: `convert_rebalance_plan_to_rows()` (Line 58)

### Supported Chains

The implementation supports all chains in the repository:
- Ethereum (ETH)
- Base
- Sonic
- Arbitrum
- Plasma
- Linea

---

## Documentation Improvements Made

To make the implementation explicit and prevent future confusion, comprehensive documentation was added:

### 1. Module-Level Documentation

Added a detailed docstring at the top of `update_blocks.py` explaining:
- The purpose of the module
- **Explicit statement that Alchemy's deprecated endpoint is NOT used**
- List of APIs used (Etherscan primary, DeFi Llama fallback)
- References to API documentation

**Citation Source:** `update_blocks.py` Lines 18-31

### 2. Enhanced Function Docstrings

Updated both `get_block_by_timestamp_etherscan()` and `get_block_by_timestamp_defi_llama()` with:
- Comprehensive parameter descriptions
- Return value documentation
- Implementation notes
- API references
- **Explicit mention of Alchemy deprecation notice**

**Citation Sources:**
- `get_block_by_timestamp_etherscan()` docstring: Lines 120-140
- `get_block_by_timestamp_defi_llama()` docstring: Lines 155-176

---

## Verification

### Syntax Check
✅ Python syntax verified using `py_compile` - No errors

### Code Review
✅ All changes are documentation-only, no functional changes made

### API Verification
✅ Confirmed codebase uses:
- Etherscan API (recommended by Alchemy)
- DeFi Llama API (reliable fallback)
- **NOT** Alchemy's deprecated endpoint

---

## Conclusion

**The Tokemak v2-rebalance-dashboard repository is fully compliant with Alchemy's deprecation notice.**

The codebase was already using Etherscan's Block Number by Timestamp API (the recommended alternative) as its primary source before the deprecation notice was issued. No code changes are required.

Documentation has been added to make this implementation explicit and prevent future confusion.

---

## References

1. **Etherscan Block Number by Timestamp API**  
   https://docs.etherscan.io/api-endpoints/blocks#get-block-number-by-timestamp

2. **DeFi Llama Blocks API**  
   https://defillama.com/docs/api

3. **Alchemy Deprecation Notice** (Referenced in code documentation)
   - Endpoint: "Blocks by Timestamp"
   - Deprecation Date: December 15, 2025
   - Recommended Alternative: Etherscan Block Number by Timestamp API

4. **Implementation Files**
   - Primary: `mainnet_launch/database/schema/ensure_tables_are_current/using_onchain/helpers/update_blocks.py`
   - Consumer: `mainnet_launch/database/schema/ensure_tables_are_current/using_rebalance_plans/update_destination_states_from_rebalance_plan.py`

---

**Investigation Completed By:** GitHub Copilot  
**Date:** December 17, 2025
