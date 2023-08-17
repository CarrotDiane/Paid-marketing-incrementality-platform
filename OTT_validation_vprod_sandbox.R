library('odbc')
library('radish')
library(dplyr)
install.packages("grf")
library('grf')


con <- snowflake_connect()

snowflake_db_write_table <- function(
  conn, table_name, df,
  temporary = FALSE, field.types = NULL
) {
  on_error <- "abort_statement"
  compression <- "snappy"

  stopifnot(
    rlang::is_scalar_logical(temporary) &&
      !is.na(temporary), rlang::is_null(field.types) ||
      (rlang::is_named(field.types))
  )

  # create the empty destination table
  create_table_query <- DBI::sqlCreateTable(
    conn, DBI::SQL(table_name), df,
    field.types = field.types, row.names = FALSE,
    temporary = temporary
  )
  DBI::dbExecute(conn, create_table_query, immediate = TRUE)

  # upload dataframe as parquet file to a snowflake stage
  stage_name <- stringr::str_c(sample(base::letters, 15, replace = TRUE), collapse = "")
  tf <- NULL  # ignore warning "no visible binding for global variable 'tf'"
  DBI::dbExecute(conn, glue::glue("CREATE STAGE \"{stage_name}\""))
  withr::with_tempfile("tf", {
    arrow::write_parquet(df, tf, compression = compression)
    query <- glue::glue("PUT file://{tf} @\"{stage_name}\"")
    print(query)
    DBI::dbExecute(conn, query)
  })

  # copy from stage into destination table
  copy_query <- glue::glue("COPY INTO {table_name}
    FROM @\"{stage_name}\"
    FILE_FORMAT=(TYPE=parquet COMPRESSION={compression})
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
    PURGE=TRUE ON_ERROR={on_error}")
  print(copy_query)
  DBI::dbExecute(conn, copy_query)
  invisible(TRUE)
}

# ============================================matched data iorder======================================================
data <- run_query("select distinct cohort from sandbox_db.dianedou.OTT_matching_attributes_vprod_matched", .con = con)
data_outcome <-run_query("select distinct reporting_month from sandbox_db.dianedou.OTT_reporting_metrics_orders_28d_vprod", .con = con)

query_data = 'select distinct * from sandbox_db.dianedou.OTT_matching_attributes_vprod_matched where cohort ='
query_outcome = 'select distinct * from sandbox_db.dianedou.OTT_reporting_metrics_orders_28d_vprod_joined where cohort ='

feature = c('total_spend_fav_store_last_five_deliveries'
            , 'tot_visit_time'
            , 'n_platforms_visited_l28'
            , 'days_signup_to_activation'
            , 'min_days_between_reorder_l28'
            , 'avg_initial_charge_amt_usd_fav_store_last_five_deliveries'
            , 'total_items_fav_store_last_five_deliveries'
            , 'days_since_last_order_fav_store_last_five_deliveries'
            , 'min_days_between_reorder'
            , 'days_since_last_completed_order'
            , 'avg_change_tip_pct'
            , 'past_trial'
            , 'tip'
            , 'is_wao'
            , 'is_hao'
            , 'gtv_l28'
            , 'gtv_l91'
            , 'gtv_lifetime'
            , 'deliveries_l28'
            , 'deliveries_lifetime'
            , 'visits_l28'
            , 'visits_l91'
            , 'visits_lifetime'
            , 'signup_days'
            , 'zip_median_income')

feature[!feature %in% colnames(data)]
# data[, feature] <- sapply(data[, feature], as.numeric)

cohort_lst = sort(unique(data['cohort'])[,1])
reporting_month_lst = sort(unique(data_outcome['reporting_month'])[,1])

cohort_lst
reporting_month_lst

tab = matrix(, ncol=5, byrow=TRUE)
colnames(tab) <- c('metric', 'cohort', 'reporting_month', 'estimate', 'std_err')

data = c()
data_outcome = c()

dr_estimate <- function(data_frame, feature_set, target_variable, tab, cohort, reporting_month, variant_variable = 'variant.x') {

  # data_frame = data[sample( nrow(data), 20000), ]
  # data_frame = data

  X = data_frame[feature_set]
  # Y = data_frame[,c(target_variable)]
  Y = as.numeric(data_frame[,c(target_variable)])

  data_frame[,c(target_variable)] = as.numeric(data_frame[,c(target_variable)])

  W = data_frame[,c(variant_variable)]

  # W = sapply(W, vetorize)

  # tau.forest <- causal_forest(X, Y, W)

  forest.W <- regression_forest(X, W, tune.parameters = "all")

  W.hat <- predict(forest.W)$predictions

  forest.Y <- regression_forest(X, Y, tune.parameters = "all")

  Y.hat <- predict(forest.Y)$predictions

  # forest.Y.varimp <- variable_importance(forest.Y)

  # Note: Forests may have a hard time when trained on very few variables
  # (e.g., ncol(X) = 1, 2, or 3). We recommend not being too aggressive
  # in selection.

  # selected.vars <- which(forest.Y.varimp / mean(forest.Y.varimp) > 0.2)


  tau.forest <- causal_forest(X, Y, W, #X[, selected.vars]
                              W.hat = W.hat, Y.hat = Y.hat,
                              tune.parameters = "all")

  temp_C = average_treatment_effect(tau.forest, target.sample = "overlap")

  reporting_month = as.Date(as.integer(reporting_month), origin="1970-01-01")
  new_row = c(target_variable, cohort, reporting_month, temp_C[1], temp_C[2])
  tab <- rbind(tab, new_row)

  forest.W = c()
  forest.Y = c()
  tau.forest = c()

  return(tab)
}



for (i in (1:length(cohort_lst))) { #
  cohort_temp = (cohort_lst[i])
  print(cohort_temp)

  query_data_temp = paste(query_data,toString(shQuote(cohort_temp)))
  # data_cohort = data[data$cohort== cohort_temp, ]
  data_cohort <- run_query(query_data_temp)
  data_cohort[, feature] <- sapply(data_cohort[, feature], as.numeric)
  # print(dim(data_eb_cohort))
  for (j in (1: (length(reporting_month_lst) ))){
    date_temp = (reporting_month_lst[j])
    print(date_temp)
    if ((as.Date(date_temp)) <= as.Date(cohort_temp)) next
    # if ((date_temp  %in% tab[tab[, 'cohort'] == cohort_temp, 'reporting_month']) & (cohort_temp  %in% tab[, 'cohort']))  next
    # print(as.Date(date_temp)-6)
    # outcome_cohort = data_outcome[(as.Date(data_outcome$cohort) == as.Date(cohort_temp)) & (as.Date(data_outcome$reporting_month) == as.Date(date_temp)), ]

    query_outcome_temp = paste(query_outcome, toString(shQuote(cohort_temp)), 'and reporting_month =', toString(shQuote(date_temp)))
    outcome_cohort = run_query(query_outcome_temp)

    df_merge = merge(data_cohort, outcome_cohort, by = "user_id")

    # data_cohort = c()
    outocme_cohort = c()

    for (metric in c('l28d_orders')){
      tab = dr_estimate(df_merge, feature, metric, tab, cohort_temp, date_temp)

      forest.W = c()
      forest.Y = c()
      tau.forest = c()
      gc()
    write.csv(tab, paste('OTT_iorder-cohorts-', cohort_temp, '.csv'))
    df_merge = c()
    gc()

    }
  }
}

tab

tab_copy <- data.frame(tab)
tab_copy <- tab_copy[c(2:nrow(tab_copy)),]
tab_copy <- tab_copy[,c(3:ncol(tab_copy))]
tab_copy

if (TRUE) {
  library(tidyverse)
  # library(radish)

  mydf <- tab_copy

  sbconn <- snowflake_connect(role = "INSTACART_DEVELOPER_ROLE", database = "SANDBOX_DB", schema = "DIANEDOU")

  snowflake_db_write_table(
    conn = sbconn,
    table_name = "OTT_DR_iOrder_vprod",
    df = mydf
  )
}


# ============================================matched data igtv======================================================
data <- run_query("select distinct cohort from sandbox_db.dianedou.OTT_matching_attributes_vprod_matched", .con = con)
data_outcome <-run_query("select distinct reporting_month from sandbox_db.dianedou.OTT_reporting_metrics_28d_vprod_joined ", .con = con)

query_data = 'select distinct * from sandbox_db.dianedou.OTT_matching_attributes_vprod_matched where cohort ='
query_outcome = 'select distinct * from sandbox_db.dianedou.OTT_reporting_metrics_28d_vprod_joined where cohort ='

feature = c('total_spend_fav_store_last_five_deliveries'
            , 'tot_visit_time'
            , 'n_platforms_visited_l28'
            , 'days_signup_to_activation'
            , 'min_days_between_reorder_l28'
            , 'avg_initial_charge_amt_usd_fav_store_last_five_deliveries'
            , 'total_items_fav_store_last_five_deliveries'
            , 'days_since_last_order_fav_store_last_five_deliveries'
            , 'min_days_between_reorder'
            , 'days_since_last_completed_order'
            , 'avg_change_tip_pct'
            , 'past_trial'
            , 'tip'
            , 'is_wao'
            , 'is_hao'
            , 'gtv_l28'
            , 'gtv_l91'
            , 'gtv_lifetime'
            , 'deliveries_l28'
            , 'deliveries_lifetime'
            , 'visits_l28'
            , 'visits_l91'
            , 'visits_lifetime'
            , 'signup_days'
            , 'zip_median_income')

feature[!feature %in% colnames(data)]
# data[, feature] <- sapply(data[, feature], as.numeric)

cohort_lst = sort(unique(data['cohort'])[,1])
reporting_month_lst = sort(unique(data_outcome['reporting_month'])[,1])

cohort_lst
reporting_month_lst

tab = matrix(, ncol=5, byrow=TRUE)
colnames(tab) <- c('metric', 'cohort', 'reporting_month', 'estimate', 'std_err')

data = c()
data_outcome = c()

dr_estimate <- function(data_frame, feature_set, target_variable, tab, cohort, reporting_month, variant_variable = 'variant.x') {

  # data_frame = data[sample( nrow(data), 20000), ]
  # data_frame = data

  X = data_frame[feature_set]
  # Y = data_frame[,c(target_variable)]
  Y = as.numeric(data_frame[,c(target_variable)])

  data_frame[,c(target_variable)] = as.numeric(data_frame[,c(target_variable)])

  W = data_frame[,c(variant_variable)]

  # W = sapply(W, vetorize)

  # tau.forest <- causal_forest(X, Y, W)

  forest.W <- regression_forest(X, W, tune.parameters = "all")

  W.hat <- predict(forest.W)$predictions

  forest.Y <- regression_forest(X, Y, tune.parameters = "all")

  Y.hat <- predict(forest.Y)$predictions

  # forest.Y.varimp <- variable_importance(forest.Y)

  # Note: Forests may have a hard time when trained on very few variables
  # (e.g., ncol(X) = 1, 2, or 3). We recommend not being too aggressive
  # in selection.

  # selected.vars <- which(forest.Y.varimp / mean(forest.Y.varimp) > 0.2)

  tau.forest <- causal_forest(X, Y, W, #X[, selected.vars]
                              W.hat = W.hat, Y.hat = Y.hat,
                              tune.parameters = "all")

  temp_C = average_treatment_effect(tau.forest, target.sample = "overlap")

  # reporting_month = as.Date(as.integer(reporting_month), origin="1970-01-01")
  new_row = c(target_variable, cohort, reporting_month, temp_C[1], temp_C[2])
  # print(new_row)
  tab <- rbind(tab, new_row)

  forest.W = c()
  forest.Y = c()
  tau.forest = c()

  return(tab)
}



for (i in (1:length(cohort_lst))) { #
  cohort_temp = (cohort_lst[i])
  print(cohort_temp)

  query_data_temp = paste(query_data,toString(shQuote(cohort_temp)))
  # data_cohort = data[data$cohort== cohort_temp, ]
  data_cohort <- run_query(query_data_temp)
  data_cohort[, feature] <- sapply(data_cohort[, feature], as.numeric)
  # print(dim(data_eb_cohort))
  for (j in (1: (length(reporting_month_lst) ))){
    date_temp = (reporting_month_lst[j])
    print(date_temp)
    if ((as.Date(date_temp)) <= as.Date(cohort_temp)) next
    # if ((date_temp  %in% tab[tab[, 'cohort'] == cohort_temp, 'reporting_month']) & (cohort_temp  %in% tab[, 'cohort']))  next
    # print(as.Date(date_temp)-6)
    # outcome_cohort = data_outcome[(as.Date(data_outcome$cohort) == as.Date(cohort_temp)) & (as.Date(data_outcome$reporting_month) == as.Date(date_temp)), ]

    query_outcome_temp = paste(query_outcome, toString(shQuote(cohort_temp)), 'and reporting_month =', toString(shQuote(date_temp)))
    outcome_cohort = run_query(query_outcome_temp)

    df_merge = merge(data_cohort, outcome_cohort, by = "user_id")


    # data_cohort = c()
    outocme_cohort = c()

    for (metric in c('reporting_gtv_l28')){
      tab = dr_estimate(df_merge, feature, metric, tab, cohort_temp, date_temp)

      forest.W = c()
      forest.Y = c()
      tau.forest = c()
      gc()
    # write.csv(tab, paste('SEM_iorder-cohorts-', cohort_temp, '.csv'))
    df_merge = c()
    gc()

    }
  }
}

tab

tab_copy <- data.frame(tab)
tab_copy <- tab_copy[c(104:nrow(tab_copy)),]
tab_copy <- tab_copy[,c(1:ncol(tab_copy)-1)]
tab_copy

if (TRUE) {
  library(tidyverse)
  # library(radish)

  mydf <- tab_copy

  sbconn <- snowflake_connect(role = "INSTACART_DEVELOPER_ROLE", database = "SANDBOX_DB", schema = "DIANEDOU")

  snowflake_db_write_table(
    conn = sbconn,
    table_name = "OTT_DR_iGTV_vprod",
    df = mydf
  )
}
