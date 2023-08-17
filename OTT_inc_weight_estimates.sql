//================================================================================================================================================
//=========================================================== inc weights generation =============================================================
//================================================================================================================================================
CREATE OR REPLACE TABLE sandbox_db.dianedou.temp_oad_visit_base AS (
SELECT distinct
  oad.visit_token,
  oad.omni_user_id as user_id,
  oad.visitor_type,
  date_trunc('day', OMNI_DATE_TIME_PT) as visit_date,

  case when oad.channel != 'Paid' then 'Non-Paid'
     when oad.paid_channel = 'Search' then 'SEM'--iff(oad.paid_network_name = 'Google Search', 'Search: Google', 'Search: Other')
     else oad.paid_channel end as paid_channel, --append network_name to paid_channel for Google Search
  CASE WHEN d.value:delivery_state = 'delivered'
    AND o.value:whitelabel_id = 1 --marketplace orders only
    THEN d.value:order_delivery_id END as order_delivery_id,
  CASE WHEN d.value:delivery_state = 'delivered'
    AND o.value:whitelabel_id = 1 --marketplace orders only
    THEN d.value:gtv_amt_usd END as gtv
//  o.value:order_id as order_id,
//  o.value:user_id as user_id,
//  d.value:delivery_created_date_time_pt::timestamp_ntz as delivery_created_date_time_pt,
//  d.value:delivered_date_time_pt::timestamp_ntz as delivered_date_time_pt,
//  d.value:order_delivery_gmv_amt_usd as gmv,
//  o.value:order_type::varchar as order_type,
//  o.value:zip_code::varchar as zip_code,
//case when oad.paid_channel in ('Search', 'Apple Search Ads') then oad.paid_is_branded else 0 end as paid_is_branded
FROM analysts.order_attribution_daily oad,
lateral flatten(INPUT => oad.orders) o,
lateral flatten(INPUT => o.value:deliveries) d
WHERE oad.omni_date_time_pt::date >= '2021-07-19' and visitor_type is not null
);


CREATE OR REPLACE table sandbox_db.dianedou.OTT_DR_iOrder_vprod_SE as

with base as (select "metric" as metric
, "cohort" as cohort
, "reporting_month" as reporting_month
, "estimate" as estimate
, "std_err" as std_err
from sandbox_db.dianedou.OTT_DR_iOrder_vprod
where "reporting_month" < '2023-07-23')


select reporting_month
, count(distinct cohort) as n_cohort
, coalesce(stddev(estimate) / sqrt(n_cohort) * 1.96, 0.0373340102) AS SE
-- , sqrt(sum(square(std_err))) / sqrt(n_cohort) * 1.96
-- , stddev(estimate) / sqrt(n_cohort) * 1.96
-- , coalesce(stddev(estimate) / sqrt(n_cohort) * 1.96, sqrt(sum(square(std_err))) / sqrt(n_cohort) * 1.96) as SE


from base
group by 1;


CREATE OR REPLACE table sandbox_db.dianedou.OTT_DR_iOrder_perc_vprod_v2 as

with base as (select "metric" as metric
, "cohort" as cohort
, "reporting_month" as reporting_month
, "estimate_overlap" as estimate_overlap
, "std_err_overlap" as std_err_overlap
, "estimate_att" as estimate_att
, "std_err_att" as std_err_att
from sandbox_db.dianedou.OTT_DR_iOrder_vprod_segment)


, attb as (
select b.start_date as reporting_month --date_trunc('week', visit_date) as reporting_week
, visitor_type
, paid_channel as attributed_channel
, count(distinct user_id) as ttl_visitors
, count(distinct order_delivery_id) as ttl_orders
, sum(gtv) as ttl_gtv
, ttl_gtv/ttl_visitors as gtv_per_visitor
, ttl_orders/ttl_visitors as orders_per_visitor

from sandbox_db.dianedou.temp_oad_visit_base a
left join sandbox_db.dianedou.ott_incrementality_vprod_date_ref_df b
  on a.visit_date between b.start_date and b.end_date
group by 1,2,3
having attributed_channel = 'OTT' and visitor_type = 'Existing Visitor' and reporting_month is not null
)

select
base.reporting_month
    , ttl_visitors
    , ttl_orders
    , avg(ttl_orders) as sum_orders
    -- , avg(estimate) as inc_order_per_visitor
    -- , avg(ttl_visitors) / avg(ttl_orders) as attributed_metric_impact
    -- , avg(estimate) * avg(ttl_visitors) / avg(ttl_orders) as inc_weights_before_benchmark
    -- , avg(SE) as StdE
    -- , (inc_order_per_visitor - StdE) * avg(ttl_visitors) / avg(ttl_orders) as inc_weights_lower_CI
    -- , (inc_order_per_visitor + StdE) * avg(ttl_visitors) / avg(ttl_orders) as inc_weights_upper_CI
    , avg(estimate_overlap) as inc_order_per_visitor_overlap
    , avg(ttl_visitors) / avg(ttl_orders) as attributed_metric_impact
    , avg(estimate_overlap) * avg(ttl_visitors) / avg(ttl_orders) as inc_weights_before_benchmark_overlap
    -- , avg(SE) as StdE
    -- , (inc_order_per_visitor - StdE) * avg(ttl_visitors) / avg(ttl_orders) as inc_weights_lower_CI
    -- , (inc_order_per_visitor + StdE) * avg(ttl_visitors) / avg(ttl_orders) as inc_weights_upper_CI

    , avg(estimate_att) as inc_order_per_visitor_att
    , avg(ttl_visitors) / avg(ttl_orders) as attributed_metric_impact
    , avg(estimate_att) * avg(ttl_visitors) / avg(ttl_orders) as inc_weights_before_benchmark_att
    -- , avg(SE) as StdE
    -- , (inc_order_per_visitor - StdE) * avg(ttl_visitors) / avg(ttl_orders) as inc_weights_lower_CI
    -- , (inc_order_per_visitor + StdE) * avg(ttl_visitors) / avg(ttl_orders) as inc_weights_upper_CI


from base
join attb
on base.reporting_month = attb.reporting_month
left join sandbox_db.dianedou.OTT_DR_iOrder_vprod_SE b
on base.reporting_month = b.reporting_month
group by 1,2,3

;
