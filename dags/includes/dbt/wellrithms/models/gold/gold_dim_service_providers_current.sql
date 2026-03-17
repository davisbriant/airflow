{{
  config(
    table_type='iceberg',
    unique_key='item_key',
    partitioned_by=['service_provider'],
    format='parquet'
  )
}}

with claim_items_current as (

  select * from {{ ref('silver_test_data_current') }}

),

final as (

  select 
  item_key,
  service_provider,
  service_address_3,
  service_address_2
  from 
  claim_items_current
  group by 
  item_key,
  service_provider,
  service_address_3,
  service_address_2

)

select 
* 
from 
final