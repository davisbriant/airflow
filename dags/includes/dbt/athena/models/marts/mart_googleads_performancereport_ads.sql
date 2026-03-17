{{
  config(
    materialized='incremental',
    unique_key = 'pkey',
    partition_by = ['date'],
    table_type='iceberg',
    format='parquet',
    incremental_strategy='merge',
  )
}}


with dims as (

select
adid as adid2,
type,
status,
finalurls
from
{{ ref('dim_googleads_performancereport_ads')}}
group by
1,2,3,4

),
facts as (

select 
*
from 
{{ ref('fact_googleads_performancereport_ads')}}

),

joined as (

select 
facts.*,
dims.*
from 
facts
LEFT JOIN 
dims
on facts.adid = dims.adid2
)

select 
* 
from 
TABLE(exclude_columns(input => table(joined),columns => DESCRIPTOR(adid2)))