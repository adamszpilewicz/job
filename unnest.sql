with table_01 as
(
          select *
          from
            (select split(replace(good_bad_odds,  ',',';'), ';') as good_bad_odds,
                    split(replace(feature,        '",','";'), ';') as feature,
                    split(replace(attribute,      '",','";'), ';') as attribute,
                    split(replace(WoE_current,    ',',';'), ';') as WoE_current,
                    split(replace(IV_current,     ',',';'), ';') as IV_current,
                    split(replace(num_loans,      ',',';'), ';') as num_loans,
                    split(replace(num_good_loans, ',',';'), ';') as num_good_loans,
                    split(replace(num_bad_loans,  ',',';'), ';') as num_bad_loans,
                    split(replace(perc_of_loans,   ',',';'), ';') as perc_of_loans,
                    split(replace(perc_of_good,    ',',';'), ';') as perc_of_good,
                    split(replace(perc_of_bad,     ',',';'), ';') as perc_of_bad,
                    split(replace(gini,           ',',';'), ';') as gini,
                    split(replace(ks,             ',',';'), ';') as ks,
                    split(replace(auroc,          ',',';'), ';') as auroc,
                    start_date,
                    pk_model_statistics_id,
                    end_date,
                    bad_event_type,
                    observation_period,
--                     min_obs_period,
                    credit_score_version,
                    record_creation_date
            from
              (
              select    JSON_EXTRACT(credit_model_statistics, '$.good_bad_odds') as good_bad_odds,
                        JSON_EXTRACT(credit_model_statistics, '$.feature') as feature,
                        JSON_EXTRACT(credit_model_statistics, '$.attribute') as attribute,
                        JSON_EXTRACT(credit_model_statistics, '$.WoE_current') as WoE_current,
                        JSON_EXTRACT(credit_model_statistics, '$.IV_current') as IV_current,
                        JSON_EXTRACT(credit_model_statistics, '$.num_loans') as num_loans,
                        JSON_EXTRACT(credit_model_statistics, '$.num_good_loans') as num_good_loans,
                        JSON_EXTRACT(credit_model_statistics, '$.num_bad_loans') as num_bad_loans,
                        JSON_EXTRACT(credit_model_statistics, '$.percent_of_loans') as perc_of_loans,
                        JSON_EXTRACT(credit_model_statistics, '$.percent_of_good') as perc_of_good,
                        JSON_EXTRACT(credit_model_statistics, '$.percent_of_bad') as perc_of_bad,
                        JSON_EXTRACT(credit_model_statistics, '$.gini') as gini,
                        JSON_EXTRACT(credit_model_statistics, '$.ks') as ks,
                        JSON_EXTRACT(credit_model_statistics, '$.auroc') as auroc,
                        -- [
                        start_date,
                        -- ] as start_date,
                        -- [
                        end_date,
                        -- ] as end_date,
                        -- [
                        pk_model_statistics_id,
                        -- ] as pk_model_statistics_id,
                        -- [
                        bad_event_type,
                        -- ] as bad_event_type,
                        -- [
                        observation_period,
                        -- ] as observation_period,
--                         xx lack in the columns
--                         [
--                         min_obs_period
--                         ] as min_obs_period,
                        -- [
                        credit_score_version,
                        -- ] as credit_score_version,
                        -- [
                        record_creation_date
                        -- ] as record_creation_date


              from
                (
                select  *
                from `dl_layer2.fac_retail_credit_model_portfolio_statistics`
                where record_creation_date  >= '2019-12-16 12:00:56.465810 UTC'
                )
              )
            )
        ),

      split as
        (
          select    good_bad_odds, index_good_bad_odds, feature, index_feature, attribute, index_attribute,
                    WoE_current, index_WoE_current, IV_current, index_IV_current, num_loans, index_num_loans,
                    num_good_loans, index_num_good_loans, num_bad_loans, index_num_bad_loans,
                    perc_of_loans, index_perc_of_loans, perc_of_good, index_perc_of_good, perc_of_bad, index_perc_of_bad,
                    gini, index_gini, ks, index_ks, auroc, index_auroc,

                    --not nested variables
                    start_date,
                    pk_model_statistics_id,
                    end_date,
                    bad_event_type,
                    observation_period,
--                     min_obs_period,
                    credit_score_version,
                    record_creation_date


          from      table_01,
                    unnest(good_bad_odds) good_bad_odds with offset as index_good_bad_odds ,
                    unnest(feature) feature with offset as index_feature,
                    unnest(attribute) attribute with offset as index_attribute,
                    unnest(WoE_current) WoE_current with offset as index_WoE_current,
                    unnest(IV_current) IV_current with offset as index_IV_current,
                    unnest(num_loans) num_loans with offset as index_num_loans,
                    unnest(num_good_loans) num_good_loans with offset as index_num_good_loans,
                    unnest(num_bad_loans) num_bad_loans with offset as index_num_bad_loans,
                    unnest(perc_of_loans) perc_of_loans with offset as index_perc_of_loans,
                    unnest(perc_of_good) perc_of_good with offset as index_perc_of_good,
                    unnest(perc_of_bad) perc_of_bad with offset as index_perc_of_bad,
                    unnest(gini) gini with offset as index_gini,
                    unnest(ks) ks with offset as index_ks,
                    unnest(auroc) auroc with offset as index_auroc

        )

select
        start_date,
        cast(pk_model_statistics_id as string) as pk_model_statistics_id,
        end_date,
        bad_event_type,
        observation_period,
--         min_obs_period,
        credit_score_version,
        record_creation_date,
        replace(replace(good_bad_odds, "[", ""), "]","") as good_bad_odds,
        replace(replace(feature, "[", ""), "]","") as feature,
        replace(replace(attribute, "[", ""), "]","") as attribute,
        replace(replace(WoE_current, "[", ""), "]","") as WoE_current,
        replace(replace(IV_current, "[", ""), "]","") as IV_current,
        replace(replace(num_loans, "[", ""), "]","") as num_loans,
        replace(replace(num_good_loans, "[", ""), "]","") as num_good_loans,
        replace(replace(num_bad_loans, "[", ""), "]","") as num_bad_loans,
        replace(replace(perc_of_loans, "[", ""), "]","") as perc_of_loans,
        replace(replace(perc_of_good, "[", ""), "]","") as perc_of_good,
        replace(replace(perc_of_bad, "[", ""), "]","") as perc_of_bad,
        replace(replace(gini, "[", ""), "]","") as gini,
        replace(replace(ks, "[", ""), "]","") as ks,
        replace(replace(auroc, "[", ""), "]","") as auroc

from    split
where   index_good_bad_odds = index_feature
AND     index_feature = index_attribute
and     index_attribute = index_WoE_current
AND     index_WoE_current = index_IV_current
and     index_IV_current = index_num_loans
and     index_num_loans = index_num_good_loans
and     index_num_good_loans = index_num_bad_loans
and     index_num_bad_loans = index_perc_of_loans
AND     index_perc_of_loans = index_perc_of_good
and     index_perc_of_good = index_perc_of_bad
and     index_perc_of_bad = index_gini
AND     index_gini = index_ks
AND     index_ks = index_auroc


order by record_creation_date desc


