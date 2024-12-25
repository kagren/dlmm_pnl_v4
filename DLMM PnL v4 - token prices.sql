/* Get USD and SOL prices for all known mints and timestamps (both for those that we 
already have prices for, and those timestamps that we have DLMM actions for) */
with 
token_prices as (
    select 
        minute,
        mint,
        avg_price_usd,
        avg_price_sol
    from dune.kagren0.result_token_prices_1_minute_resolution q
    where mint in (
        select account_tokenMintX from dune.kagren0.result_dlmm_pn_l_v_4_realized_positions_actions
    ) or mint in (
        select account_tokenMintY from dune.kagren0.result_dlmm_pn_l_v_4_realized_positions_actions
    )
),
base_prices as (
    select
        coalesce(pa.minute, tp.minute) minute,
        coalesce(tp.mint, pa.mint) mint,
        tp.avg_price_usd,
        tp.avg_price_sol
    from (
        select 
            distinct 
            account_tokenMintX mint, 
            call_block_time_minute minute
        from dune.kagren0.result_dlmm_pn_l_v_4_realized_positions_actions
        union distinct
        select 
            distinct 
            account_tokenMintY miunt,
            call_block_time_minute
        from dune.kagren0.result_dlmm_pn_l_v_4_realized_positions_actions
        
    ) pa
    full outer join token_prices tp on
        tp.mint = pa.mint
        and tp.minute = pa.minute
),
base_prices_fill_null_usd as (
    /* If USD price is not known, derive it from the SOL price */
    select
        bp.minute,
        bp.mint,
        coalesce(bp.avg_price_usd, bp.avg_price_sol * bp_sol.avg_price_usd) avg_price_usd,
        bp.avg_price_sol
    from base_prices bp
    left join base_prices bp_sol on 
        bp.minute = bp_sol.minute 
        and bp_sol.mint = 'So11111111111111111111111111111111111111112'
        and bp.avg_price_usd is null
)
select
    minute,
    mint,
    avg_price_usd,
    lead(avg_price_usd, 1) ignore nulls over(partition by mint order by minute) avg_price_usd_prev,
    lag(avg_price_usd, 1) ignore nulls over(partition by mint order by minute) avg_price_usd_next,
    avg_price_sol,
    lead(avg_price_sol, 1) ignore nulls over(partition by mint order by minute) avg_price_sol_prev,
    lag(avg_price_sol, 1) ignore nulls over(partition by mint order by minute) avg_price_sol_next

from base_prices_fill_null_usd