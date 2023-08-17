//================================================================================================================================================
//=========================================================== outcome generation =================================================================
//================================================================================================================================================

CREATE OR REPLACE table  sandbox_db.dianedou.OTT_reporting_metrics_28d_vprod_temp as

with base as (select distinct * from table($user_base_table) where cohort = $cohort_temp) --sample(10)

, cohort_ref as (select distinct activation_date_pt as cohort_start, dateadd('day', 27, activation_date_pt) as cohort_end from dwh.dim_user where activation_date_pt >= $cohort_temp and mod(datediff('day', $cohort_temp, activation_date_pt), 28) = 0 order by 1)

, fuga as (select * from INSTADATA.dwh.fact_user_growth_accounting fuga where fuga.full_date_pt >= $cohort_temp)

  SELECT base.user_id,
  base.variant,
  base.created_at_pt,
  base.wk_ref,
  base.cohort,
  cohort_ref.cohort_start as reporting_month,
  fuga.is_mao as reporting_is_mao,
  COALESCE(fuga.gtv_l28:overall,0) as reporting_gtv_l28
  FROM  base
  left join cohort_ref on cohort_start > base.cohort
  left join fuga ON base.user_id = fuga.user_id
  AND fuga.full_date_pt = cohort_end
;

CREATE OR REPLACE table sandbox_db.dianedou.OTT_reporting_metrics_orders_28d_vprod_temp as

(with base as
    (select omni_user_id as user_id,
           o.value:deliveries  as deliveries,
     O.VALUE:order_type as order_type,
     o.value:order_id as order_id,
     o.value:order_created_date_time_pt as order_created_date_time_pt
    from analysts.order_attribution_daily oad,
     lateral flatten(INPUT => oad.orders) o
    where (VISIT_START_DATE_TIME_PT  >= $cohort_temp)
    and completed_deliveries > 0
    and whitelabel_ind = 'N'
    )

 , cohort_ref as (select distinct activation_date_pt as cohort_start, dateadd('day', 27, activation_date_pt) as cohort_end from dwh.dim_user where activation_date_pt >= $cohort_temp and mod(datediff('day', $cohort_temp, activation_date_pt), 28) = 0 order by 1)

    SELECT DISTINCT map.user_id
    , map.variant
    , map.created_at_pt
    , map.wk_ref
    , map.cohort
    , cohort_ref.cohort_start as reporting_month
    , count (distinct order_id) as L28D_orders
    FROM (select distinct * from table($user_base_table) where cohort = $cohort_temp) map --sample(10)
    left join cohort_ref on cohort_start > map.cohort
    LEFT JOIN base
        ON map.user_id = base.user_id
        AND date_trunc('day', order_created_date_time_pt::datetime) >= cohort_start
        AND date_trunc('day', order_created_date_time_pt::datetime) < cohort_end
    GROUP BY 1,2,3,4,5,6

);
