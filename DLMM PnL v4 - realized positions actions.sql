/*
Part of the queries needed to calculate PnL for Meteora DLMM positions / users. This query will return all the
actions (i.e. create position, swap, collect fees, close position) along with the USD price at the time of when
the action occurred.

Author: x.com/kagren0
*/

with
liquidity_adds_base as (
    select
        call_tx_index,
        call_block_slot,
        call_block_time,
        call_block_date,
        call_outer_instruction_index,
        call_inner_instruction_index,
        account_lbPair,
        account_position

    from dlmm_solana.lb_clmm_call_addLiquidity

    union all

    select
        call_tx_index,
        call_block_slot,
        call_block_time,
        call_block_date,
        call_outer_instruction_index,
        call_inner_instruction_index,
        account_lbPair,
        account_position

    from dlmm_solana.lb_clmm_call_addLiquidityByWeight
    
    union all
    
    select
        call_tx_index,
        call_block_slot,
        call_block_time,
        call_block_date,
        call_outer_instruction_index,
        call_inner_instruction_index,
        account_lbPair,
        account_position
        
    from dlmm_solana.lb_clmm_call_addLiquidityOneSide a
    
    union all

    select
        call_tx_index,
        call_block_slot,
        call_block_time,
        call_block_date,
        call_outer_instruction_index,
        call_inner_instruction_index,
        account_lbPair,
        account_position
        
    from dlmm_solana.lb_clmm_call_addLiquidityByStrategyOneSide a
    
    union all

    select
        call_tx_index,
        call_block_slot,
        call_block_time,
        call_block_date,
        call_outer_instruction_index,
        call_inner_instruction_index,
        account_lbPair,
        account_position
        
    from dlmm_solana.lb_clmm_call_addLiquidityByStrategy a
    
),
/*
 * Get all the liquidity removals per position
 */ 
liquidity_removals_base as (
    select
        call_tx_index,
        call_block_slot,
        call_block_time,
        call_block_date,
        call_outer_instruction_index,
        call_inner_instruction_index,
        account_lbPair,
        account_position

    from dlmm_solana.lb_clmm_call_removeAllLiquidity

    union all

    select
        call_tx_index,
        call_block_slot,
        call_block_time,
        call_block_date,
        call_outer_instruction_index,
        call_inner_instruction_index,
        account_lbPair,
        account_position

    from dlmm_solana.lb_clmm_call_removeLiquidity
    
    union all
    
    select
        call_tx_index,
        call_block_slot,
        call_block_time,
        call_block_date,
        call_outer_instruction_index,
        call_inner_instruction_index,
        account_lbPair,
        account_position
        
    from dlmm_solana.lb_clmm_call_removeLiquidityByRange
    
),
liquidity_adds as (
    select 
        'ADD' action,
        a.*
    from liquidity_adds_base a
),
liquidity_removals as (
    select 
        'REMOVE' action,
        a.*
    from liquidity_removals_base a
),
position_reward_claims as (
    select 
        'CLAIM_REWARD' action,
        cr.call_tx_index,
        cr.call_block_slot,
        cr.call_block_time,
        cr.call_block_date,
        cr.call_outer_instruction_index,
        cr.call_inner_instruction_index,
        cr.account_lbPair,
        cr.account_position
    from dlmm_solana.lb_clmm_call_claimReward cr
),
position_fee_claims as (
    select 
        'CLAIM_FEE' action,
        cr.call_tx_index,
        cr.call_block_slot,
        cr.call_block_time,
        cr.call_block_date,
        cr.call_outer_instruction_index,
        cr.call_inner_instruction_index,
        cr.account_lbPair,
        cr.account_position
    from dlmm_solana.lb_clmm_call_claimFee cr
),
position_closings as (
    select 
        'CLOSE' action,
        cp.call_tx_index,
        cp.call_block_slot,
        cp.call_block_time,
        cp.call_block_date,
        cp.call_outer_instruction_index,
        cp.call_inner_instruction_index,
        cp.account_lbPair,
        cp.account_position
    from dlmm_solana.lb_clmm_call_closePosition cp
),
all_actions_for_closed_positions as (
    select 
        action,
        call_tx_index,
        call_block_slot,
        call_block_time,
        call_block_date,
        call_outer_instruction_index,
        call_inner_instruction_index,
        account_lbPair,
        --account_position_rank,
        account_position
        --rank() over (partition by account_lbPair order by account_position) account_position_rank
    from (
        select * from liquidity_adds
        union all 
        select * from liquidity_removals
        union all 
        select * from position_reward_claims
        union all
        select * from position_closings
        union all
        select * from position_fee_claims
    ) l
    /* Only return positions that have been closed */
    where exists (select 1 from position_closings q where q.account_position = l.account_position)
),
mints_and_timestamps as (
    select 
        distinct
        ilp.account_tokenMintX mint,
        date_trunc('minute', l.call_block_time) minute
        
    from all_actions_for_closed_positions l
    inner join dlmm_solana.lb_clmm_call_initializeLbPair ilp on 
            l.account_lbPair = ilp.account_lbPair

    union distinct 
    
    select 
        distinct
        ilp.account_tokenMintY mint,
        date_trunc('minute', l.call_block_time) minute
        
    from all_actions_for_closed_positions l
    inner join dlmm_solana.lb_clmm_call_initializeLbPair ilp on 
            l.account_lbPair = ilp.account_lbPair
)
select 
    l.action,
    l.call_block_date,
    l.call_block_time,
    date_trunc('minute', l.call_block_time) call_block_time_minute,
    l.call_block_slot,
    l.call_tx_index,
    l.call_outer_instruction_index,
    l.call_inner_instruction_index,
    ilp.account_lbPair,
    l.account_position,
    ilp.account_reserveX,
    ilp.account_reserveY,
    ilp.account_tokenMintX,
    ilp.account_tokenMintY
from all_actions_for_closed_positions l
inner join dlmm_solana.lb_clmm_call_initializeLbPair ilp on 
        l.account_lbPair = ilp.account_lbPair