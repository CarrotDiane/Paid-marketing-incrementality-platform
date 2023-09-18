//================================================================================================================================================
//=========================================================== outcome generation =================================================================
//================================================================================================================================================

-- CREATE OR REPLACE table  sandbox_db.dianedou.OLV_new_user_reporting_metrics_28d_vprod_temp as
--
-- with base as (select distinct * from table($user_base_table) where cohort = $cohort_temp) --sample(10)
--
-- -- , cohort_ref as (select distinct activation_date_pt as cohort_start, dateadd('day', 27, activation_date_pt) as cohort_end from dwh.dim_user where activation_date_pt >= $cohort_temp and mod(datediff('day', $cohort_temp, activation_date_pt), 28) = 0 order by 1)
--
-- , fuga as (select * from INSTADATA.dwh.fact_user_growth_accounting fuga where fuga.full_date_pt between $cohort_temp and dateadd('day', 28*2, $cohort_temp))
--
--   SELECT base.user_id,
--   base.variant,
--   base.created_at_pt,
--   base.wk_ref,
--   base.cohort,
--   base.cohort as reporting_month,
--   fuga.is_mao as reporting_is_mao,
--   COALESCE(fuga.gtv_l28:overall,0) as reporting_gtv_l28
--   FROM  base
--   -- left join cohort_ref on cohort_start >= base.cohort
--   left join fuga ON base.user_id = fuga.user_id
--   AND fuga.full_date_pt = dateadd('day', 27, base.created_at_pt)
-- ;

CREATE OR REPLACE table sandbox_db.dianedou.OLV_new_user_reporting_metrics_orders_28d_vprod_temp as

(with base as
    (select omni_user_id as user_id,
     o.value:deliveries  as deliveries,
     o.VALUE:order_type as order_type,
     o.value:order_id as order_id,
     date_trunc('day', o.value:order_created_date_time_pt::datetime) as order_created_date_pt,
     d.value:gtv_amt_usd as gtv
    from analysts.order_attribution_daily oad,
     lateral flatten(INPUT => oad.orders) o,
     lateral flatten(INPUT => o.value:deliveries) d
    where order_created_date_pt between $cohort_temp and dateadd('day', 28*2, $cohort_temp)
    and completed_deliveries > 0
    and whitelabel_ind = 'N'
    )

, base2 as
    (select base.*
    , case when base.order_id = du.ACTIVATION_ORDER_ID then 1 else 0 end as activation_flag
    from base
    left join dwh.dim_user du on base.user_id = du.user_id
    )

SELECT DISTINCT map.user_id
    , map.variant
    , map.created_at_pt
    , map.wk_ref
    , map.cohort
    , map.cohort as reporting_month
    , count(distinct case when order_id is not null then map.user_id end) as activations
    , sum(distinct case when order_id is not null then base2.gtv end) as activation_gtv
    , avg(distinct case when order_id is not null then base2.gtv end) as activation_gtv_avg

FROM (select distinct * from table($user_base_table) where cohort = $cohort_temp) map --sample(10)
    -- left join cohort_ref on cohort_start >= map.cohort
LEFT JOIN base2
    ON map.user_id = base2.user_id
    AND base2.activation_flag = 1
    AND order_created_date_pt between map.created_at_pt and dateadd('day', 27, map.created_at_pt)

GROUP BY 1,2,3,4,5,6

);
