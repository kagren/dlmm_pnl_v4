# Overview

This repo contains Dune SQL needed to calculate profit-and-loss (PnL) data for Meteora DLMM positions. It is split into multiple SQL files for both readability but also for performance as some of the SQL files have to be materialized in Dune to be able to run without reaching timeout.

# Queries

- **Token Prices 1 Minute Resolution.sql** -- Calculate the average token (on Solana) price per minute by using data from DEX swaps, in USD and in SOL. The price is calculated only for minutes where there have been at least 3 trades, and where the average price for that minute deviates less than 100% from the 15 minute average price. Uses the Dune tables dex_solana.trades (to calculate prices from swap information), spl_token_solana.spl_token_call_transferChecked and spl_token_2022_solana.spl_token_2022_call_transferChecked (to deterimne the amount of decimals a mint has, as this information is not always correctly used in dex_solana.trades)
- **DLMM PnL v4 - token prices.sql** (materialized) -- Get USD and SOL prices for all known mints and timestamps (both for those that we 
already have prices for, and those timestamps that we have DLMM actions for). Uses the "Token Prices 1 Minute Resolution.sql" query.
- **DLMM PnL v4 - realized positions actions.sql** (materialized) -- This query will return all the
actions (i.e. create position, add/remove liquidity, swap, collect fees, close position) for all DLMM pools.
- **DLMM PnL v4 - realized positions actions with prices.sql** -- Combines data from "DLMM PnL v4 - realized positions actions.sql" and "DLMM PnL v4 - token prices.sql" to return all actions for all DLMM pools, along with pricing at the time of the action.
- **DLMM PnL v4 - gains by position.sql** (materialized) -- Calculates gain by position by using the actual token amounts transferred at each action along with the pricing at that time. This will typically be the main table to query in order further aggregate/analyze profitability per wallet and/or position.

# ER Diagram
![alt text](./ER%20diagram.svg)