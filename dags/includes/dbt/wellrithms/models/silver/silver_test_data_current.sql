{{
  config(
    table_type='iceberg',
    unique_key='primary_key',
    partitioned_by=['service_provider'],
    format='parquet'
  )
}}

with versions as (

  select * from {{ ref('silver_test_data_versions') }}

),

current_version as (

  select 
  item_key,
  max(item_key_version) as most_recent_version
  from 
  versions
  group by 
  item_key

),

final as (

  select 
  versions.*
  from 
  versions
  inner join current_version 
  on versions.item_key = current_version.item_key 
  and 
  versions.item_key_version = current_version.most_recent_version

)

select 
* 
from 
final