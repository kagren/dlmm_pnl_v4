/*
Calculate the average token (on Solana) price per minute by using data from DEX swaps, in USD and in SOL. The 
price is calculated only for minutes where there have been at least 3 trades, and where the average price for
that minute deviates less than 100% from the 15 minute average price.

Author: x.com/kagren0
*/
with 
token_decimals as (
    /* Get token decimals from SPL transfer calls, since some tokens are not available in 
    the fungible tokens table or any of the initializeMint tables */
    select 
        mint, 
        arbitrary(decimals) decimals
    from (
        select account_mint mint, arbitrary(decimals) decimals from spl_token_solana.spl_token_call_transferChecked
        group by 1
        union all 
        select account_tokenMint mint, arbitrary(decimals) decimals from spl_token_2022_solana.spl_token_2022_call_transferChecked
        group by 1
    )
    group by mint
),
base_trades_per_minute as (
    select 
        t.block_month,
        date_trunc('hour', t.block_time) hour,
        date_trunc('minute', t.block_time) minute,
        t.token_bought_mint_address mint,
        sum(amount_usd) amount_usd_sum,
        /* Use token_bought_amount_raw as the decimal-adjusted token_bought_amount field is wrong for some tokens */
        sum(token_bought_amount_raw / pow(10, td.decimals)) bought_amount_sum,
        sum(case
                 when token_sold_mint_address = 'So11111111111111111111111111111111111111112' then token_sold_amount
                 else null
             end) amount_sold_sol_sum,
        sum(case
                 when token_sold_mint_address = 'So11111111111111111111111111111111111111112' then 
                     token_bought_amount_raw / pow(10, td.decimals)
                 else null
             end) amount_bought_with_sol_sum,
        count(*) trade_count
    from dex_solana.trades t
    inner join token_decimals td on t.token_bought_mint_address = td.mint
    where 
        blockchain = 'solana'
        and project in ('whirlpool', 'meteora', 'raydium')
        and amount_usd is not null
        and block_time > timestamp '2023-11-01'
    group by t.block_month, date_trunc('hour', t.block_time), date_trunc('minute', t.block_time), t.token_bought_mint_address
    having count(*) > 2
),
trades_per_minute as (
    select 
        block_month,
        hour,
        minute,
        mint,
        if(t.bought_amount_sum = 0, null, t.amount_usd_sum / t.bought_amount_sum) avg_price_usd,
        case 
            when t.mint = 'So11111111111111111111111111111111111111112' then 1.0
            else if(t.amount_bought_with_sol_sum = 0, null, t.amount_sold_sol_sum / t.amount_bought_with_sol_sum)
        end avg_price_sol,
        trade_count,
        sum(t.amount_usd_sum) over (
            partition by mint
            order by minute
            range between interval '15' minute preceding and current row
        ) / sum(t.bought_amount_sum) over (
            partition by mint
            order by minute
            range between interval '15' minute preceding and current row
        ) avg_price_usd_last_15_mins
        
    from base_trades_per_minute t
),
final_trades as (
    select 
        tm.minute,
        tm.mint,
        tm.avg_price_usd,
        tm.avg_price_sol,
        tm.trade_count trades,
        tm.avg_price_usd_last_15_mins
    from trades_per_minute tm
    where abs(tm.avg_price_usd - avg_price_usd_last_15_mins) / avg_price_usd_last_15_mins < 1.0
)
select
    minute,
    mint,
    if(is_nan(avg_price_usd) or is_infinite(avg_price_usd), null, avg_price_usd) avg_price_usd,
    if(is_nan(avg_price_sol) or is_infinite(avg_price_sol), null, avg_price_sol) avg_price_sol,
    trades,
    avg_price_usd_last_15_mins
from final_trades