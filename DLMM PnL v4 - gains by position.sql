with 
actions_with_amounts_base as (
    /*
    Get aamounts transferred to and from te pool for each action (close, add liquidity, remove liquidity) by 
    looking at the instructions in the transaction immediately following the action. For example, when someone
    adds liquidity, the instructions directly following will be the transfer of tokens from the user to the 
    pool vaults.
    */
    select 
         a.*,
         case 
            when t1.account_source = a.account_reserveX then t1.amount / power(10, t1.decimals) 
            when t2.account_source = a.account_reserveX then t2.amount / power(10, t2.decimals) 
            when t3.account_source = a.account_reserveX then t3.amount / power(10, t3.decimals) 
            when t1.account_destination = a.account_reserveX then -t1.amount / power(10, t1.decimals) 
            when t2.account_destination = a.account_reserveX then -t2.amount / power(10, t2.decimals) 
            when t3.account_destination = a.account_reserveX then -t3.amount / power(10, t3.decimals) 
        end amount_X,
         case 
            when t1.account_source = a.account_reserveY then t1.amount / power(10, t1.decimals)
            when t2.account_source = a.account_reserveY then t2.amount / power(10, t2.decimals)
            when t3.account_source = a.account_reserveY then t3.amount / power(10, t3.decimals)
            when t1.account_destination = a.account_reserveY then -t1.amount / power(10, t1.decimals)
            when t2.account_destination = a.account_reserveY then -t2.amount / power(10, t2.decimals)
            when t3.account_destination = a.account_reserveY then -t3.amount / power(10, t3.decimals)
        end amount_Y,
        row_number() over (
            partition by a.account_lbPair, a.account_position 
            order by a.call_block_slot, a.call_tx_index, a.call_outer_instruction_index, a.call_inner_instruction_index
        ) instruction_index,
        sum(case when action='CLOSE' then 1 else 0 end) over(
            partition by a.account_lbPair, a.account_position 
            order by a.call_block_slot, a.call_tx_index, a.call_outer_instruction_index, a.call_inner_instruction_index
        ) position_close_running_total
    from query_4440710 a
    left join spl_token_solana.spl_token_call_transferChecked t1 on
        /* Maybe change to join on call_block_time instead of slot as it is the partition column */
        t1.call_block_time = a.call_block_time
        and t1.call_tx_index = a.call_tx_index
        and t1.call_outer_instruction_index = a.call_outer_instruction_index
        and t1.call_inner_instruction_index = coalesce(a.call_inner_instruction_index, 0) + 1
        and (t1.account_source in (a.account_reserveX, a.account_reserveY)
            or t1.account_destination in (a.account_reserveX, a.account_reserveY))
    left join spl_token_solana.spl_token_call_transferChecked t2 on
        t2.call_block_time = a.call_block_time
        and t2.call_tx_index = a.call_tx_index
        and t2.call_outer_instruction_index = a.call_outer_instruction_index
        and t2.call_inner_instruction_index = coalesce(a.call_inner_instruction_index, 0) + 2
        and (t2.account_source in (a.account_reserveX, a.account_reserveY)
            or t2.account_destination in (a.account_reserveX, a.account_reserveY))
    left join spl_token_solana.spl_token_call_transferChecked t3 on
        t3.call_block_time = a.call_block_time
        and t3.call_tx_index = a.call_tx_index
        and t3.call_outer_instruction_index = a.call_outer_instruction_index
        and t3.call_inner_instruction_index = coalesce(a.call_inner_instruction_index, 0) + 3
        and (t3.account_source in (a.account_reserveX, a.account_reserveY)
            or t3.account_destination in (a.account_reserveX, a.account_reserveY))
),
actions_with_amounts as 
(
select 
    *,
    lag(position_close_running_total, 1, 0) over(
        partition by a.account_lbPair, a.account_position 
        order by a.call_block_slot, a.call_tx_index, a.call_outer_instruction_index, a.call_inner_instruction_index
    ) position_subgroup
    
from actions_with_amounts_base a
--where account_position = 'APnHxeqD7WkMQuVR2nkd41Nt73HPsr7eKxqBsRjbpMg7'
--order by account_lbPair, account_position, instruction_index
)
select 
    a.account_lbPair,
    a.account_position,
    a.position_subgroup,
    ip.account_owner,
    min_by(a.call_block_date, instruction_index) date_opened,
    max_by(a.call_block_date, instruction_index) date_closed,
    sum(coalesce(amount_x * price_usd_X, 0) + coalesce(amount_y * price_usd_Y, 0)) total_gains,
    sum(case 
          when action = 'ADD' then amount_x
            else null
        end
    ) adds_x,
    sum(case 
          when action = 'ADD' then amount_y
            else null
        end
    ) adds_y,
    min_by(price_usd_X, instruction_index) price_usd_X_at_open,
    min_by(price_usd_Y, instruction_index) price_usd_Y_at_open,
    max_by(price_usd_X, instruction_index) price_usd_X_at_close,
    max_by(price_usd_Y, instruction_index) price_usd_Y_at_close
from actions_with_amounts a
inner join (select distinct account_position, account_owner from dlmm_solana.lb_clmm_call_initializePosition) ip 
    on a.account_position = ip.account_position
--where 
--account_lbPair = 'DJJzSUx5gEqDJhTyE8tvZtkkHG7R7v6EoyZXN32aAsXF'
--and account_owner = '4n8hCDm8z4kJtm1WKvp4rujs6ynAiVtNpALzjuS79ZPa'
group by a.account_lbPair, a.account_position, a.position_subgroup, ip.account_owner