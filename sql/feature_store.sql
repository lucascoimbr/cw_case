with transactions as (
SELECT
    *
, to_char(transaction_date, 'HH24') transaction_hour
, (transaction_date::date - '1970-01-01'::date) / 7 AS weeks_from_epoch
, LEFT(card_number, 6) card_bin
FROM public.transactions
)

, transactions_per_hour as (
select 
*
from
(select 
transaction_hour
, max(transaction_date)
, COUNT(transaction_id) total_transactions
, COUNT(transaction_id)*1.000000/ count(distinct user_id) transactions_per_user
, (COUNT(transaction_id) FILTER (where has_cbk = 'TRUE'))*1.0000/COUNT(transaction_id) AS cbk_probability_hour
from transactions
group by 1 
) tb 
)

, distinct_cards_weeks_from_epoch as (
select 
distinct
user_id
, weeks_from_epoch
, card_number
from transactions
)

, distinct_cards_week as (
select 
e1.user_id, 
e1.weeks_from_epoch, 
count(distinct e2.card_number) distinct_cards_2_weeks
from distinct_cards_weeks_from_epoch e1 
join distinct_cards_weeks_from_epoch e2 
on abs(e1.weeks_from_epoch  - e2.weeks_from_epoch) <= 1 
and e1.user_id = e2.user_id
group by 1, 2
)

, join_distinct_cards_week as (
select 
t.*,
date_trunc('hour', transaction_date) transaction_date_hour,
distinct_cards_2_weeks
from transactions t 
left join distinct_cards_week c on c.user_id = t.user_id and c.weeks_from_epoch = t.weeks_from_epoch
)

, get_intervals as (
select 
  *,
  -- per hour per user
  SUM(CASE WHEN has_cbk = 'TRUE'  THEN 1 ELSE 0 END) OVER w1h AS num_cbk_1h,
  SUM(CASE WHEN has_cbk = 'FALSE' THEN 1 ELSE 0 END) OVER w1h AS num_not_cbk_1h,
  SUM(CASE WHEN transaction_id is not null THEN 1 ELSE 0 END) OVER w1h AS txns_by_user_last_1h,

  -- 7 days per user
  SUM(CASE WHEN has_cbk = 'TRUE'  THEN 1 ELSE 0 END) OVER w7d AS num_cbk_7d,
  SUM(CASE WHEN has_cbk = 'FALSE' THEN 1 ELSE 0 END) OVER w7d AS num_not_cbk_7d,
  SUM(CASE WHEN transaction_id is not null THEN 1 ELSE 0 END) OVER w7d AS txns_by_user_last_7d,

  -- total per user (acc)
  SUM(CASE WHEN has_cbk = 'TRUE'  THEN 1 ELSE 0 END) OVER w_overall AS user_cbk_count_lifetime,
  SUM(CASE WHEN has_cbk = 'FALSE' THEN 1 ELSE 0 END) OVER w_overall AS user_not_cbk_count_lifetime,
  SUM(CASE WHEN transaction_id is not null THEN 1 ELSE 0 END) OVER w_overall AS txns_by_user_lifetime,
	
  -- 7 days per BIN
  SUM(CASE WHEN has_cbk = 'TRUE'  THEN 1 ELSE 0 END) OVER w_overall_card_bin_7d AS num_cbk_card_bin_7d,
  SUM(CASE WHEN has_cbk = 'FALSE' THEN 1 ELSE 0 END) OVER w_overall_card_bin_7d AS num_not_cbk_card_bin_7d,
  SUM(CASE WHEN transaction_id is not null THEN 1 ELSE 0 END) OVER w_overall_card_bin_7d AS txns_card_bin_7d,

  -- total per BIN (acc)
  SUM(CASE WHEN has_cbk = 'TRUE'  THEN 1 ELSE 0 END) OVER w_overall_card_bin_overall AS num_cbk_card_bin_total,
  SUM(CASE WHEN has_cbk = 'FALSE' THEN 1 ELSE 0 END) OVER w_overall_card_bin_overall AS num_not_cbk_card_bin_total,
  SUM(CASE WHEN transaction_id is not null THEN 1 ELSE 0 END) OVER w_overall_card_bin_overall AS txns_card_bin_lifetime,

  AVG(transaction_amount) OVER w7d      AS avg_transaction_amount_7d,
  AVG(transaction_amount) OVER w_overall AS avg_transaction_amount_lifetime
FROM join_distinct_cards_week
WINDOW
   w1h AS (
    PARTITION BY user_id
    ORDER BY transaction_date
    RANGE BETWEEN INTERVAL '7 days' PRECEDING AND INTERVAL '1 milliseconds' PRECEDING
  ),
  w7d AS (
    PARTITION BY user_id
    ORDER BY transaction_date
    RANGE BETWEEN INTERVAL '7 days' PRECEDING AND INTERVAL '1 milliseconds' PRECEDING
  ),
  w_overall AS (
    PARTITION BY user_id
    ORDER BY transaction_date
    RANGE BETWEEN INTERVAL '7 days' PRECEDING AND INTERVAL '1 milliseconds' PRECEDING
  ),
  w_overall_card_bin_7d AS (
    PARTITION BY card_bin
    ORDER BY transaction_date
    RANGE BETWEEN INTERVAL '7 days' PRECEDING AND INTERVAL '1 milliseconds' PRECEDING
  ),
  w_overall_card_bin_overall AS (
    PARTITION BY card_bin
    ORDER BY transaction_date
    RANGE BETWEEN INTERVAL '7 days' PRECEDING AND INTERVAL '1 milliseconds' PRECEDING
  )
)

, get_percent_values as (
select 
gi.*
, max(txns_by_user_last_1h) over (partition by user_id, transaction_date_hour)/count(txns_by_user_last_1h) over (partition by user_id, transaction_date_hour)  txns_by_user_last_1h_hour
, num_cbk_1h*1.000000/(num_cbk_1h + greatest(num_not_cbk_1h,1)) num_cbk_1h_percent
, num_cbk_7d*1.000000/(num_cbk_7d + greatest(num_not_cbk_7d,1)) num_cbk_7d_percent
, user_cbk_count_lifetime*1.000000/(user_cbk_count_lifetime + greatest(user_not_cbk_count_lifetime,1)) user_cbk_count_lifetime_percent
, num_cbk_card_bin_7d*1.000000/(num_cbk_card_bin_7d + greatest(num_not_cbk_card_bin_7d,1)) num_cbk_card_bin_7d_percent
, num_cbk_card_bin_total*1.000000/(num_cbk_card_bin_total + greatest(num_not_cbk_card_bin_total,1)) num_cbk_card_bin_total_percent
, round(cbk_probability_hour,3) cbk_probability_hour
from get_intervals gi 
left join transactions_per_hour tph 
on gi.transaction_hour = tph.transaction_hour
)

, final_base as (
select 
user_id
, transaction_date
, distinct_cards_2_weeks
, txns_by_user_last_1h
, txns_by_user_last_7d
, num_cbk_1h_percent
, num_cbk_7d_percent
, user_cbk_count_lifetime_percent
, num_cbk_card_bin_7d_percent
, num_cbk_card_bin_total_percent
, cbk_probability_hour
, avg_transaction_amount_7d
, avg_transaction_amount_lifetime
, has_cbk
, sum(txns_by_user_last_1h_hour) * 1.0000 / count(distinct transaction_date_hour) avg_txns_by_user_1h
, max(transaction_date) max_transaction_date
from get_percent_values
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14
)

select 
	*
from 
	final_base
where transaction_date = max_transaction_date