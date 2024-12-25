/*
Part of the queries needed to calculate PnL for Meteora DLMM positions / users. This query will return all the
actions (i.e. create position, swap, collect fees, close position) along with the USD price at the time of when
the action occurred.

Author: x.com/kagren0
*/

with
token_prices as (

    select
        mint,
        minute,
        coalesce(avg_price_usd, case
            when avg_price_usd_prev is not null and avg_price_usd_next is not null 
                then (avg_price_usd_prev + avg_price_usd_next) / 2
            end, 
            avg_price_usd_prev, /* not able to take an average, use the previous value */
            avg_price_usd_next /* use the next value as a last resot */
        ) avg_price_usd,
        coalesce(avg_price_sol, case
            when avg_price_sol_prev is not null and avg_price_sol_next is not null 
                then (avg_price_sol_prev + avg_price_sol_next) / 2
            end, 
            avg_price_sol_prev, /* not able to take an average, use the previous value */
            avg_price_sol_next /* use the next value as a last resot */
        ) avg_price_sol        
    from dune.kagren0.result_dlmm_pn_l_v_4_token_prices
)
select 
    l.*,
    t_x.avg_price_usd price_usd_X,
    t_y.avg_price_usd price_usd_Y,
    t_x.avg_price_sol price_sol_X,
    t_y.avg_price_sol price_sol_Y
from dune.kagren0.result_dlmm_pn_l_v_4_realized_positions_actions l

left join token_prices t_x on 
    l.account_tokenMintX = t_x.mint
    and l.call_block_time_minute = t_x.minute
    
left join token_prices t_y on 
    l.account_tokenMintY = t_y.mint
    and l.call_block_time_minute = t_y.minute
