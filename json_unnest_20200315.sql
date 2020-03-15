with json as
(
  SELECT  
        split(json_extract(json_data, '$.page_1_gbv_nonperf.gross_balance_value'), ',') as gross_balance_value,
        split(json_extract(json_data, '$.page_1_gbv_nonperf.performing_class'), ',') as performing_class,
        split(json_extract(json_data, '$.page_1_gbv_nonperf.pk_snapshot_date'), ',') as pk_snapshot_date,
        split(json_extract(json_data, '$.page_1_gbv_nonperf.segment'), ',') as segment
  FROM (
        select * from `ordinal-crowbar-271216.dl_layer0.dim_testing`
  )
)
select cast(REGEXP_EXTRACT(replace(gross_balance_value,'}','') , r':(.*)') as float64) as gross_balance_value ,
cast(REGEXP_EXTRACT(replace(performing_class,'}','') , r':(.*)') as string) as performing_class,

    cast(REGEXP_EXTRACT(replace(replace(pk_snapshot_date,'}','') ,'"',''), r':(.*)') as string)  as pk_snapshot_date ,
cast(REGEXP_EXTRACT(replace(segment,'}','') , r':(.*)') as string) as segment 

from json, unnest(gross_balance_value) gross_balance_value with offset as pos0 ,
unnest(performing_class) performing_class with offset as pos1 ,
 unnest(pk_snapshot_date) pk_snapshot_date with offset as pos2 ,
unnest(segment) segment with offset as pos3 
where pos0=pos1
and pos1=pos2
and pos2=pos3
