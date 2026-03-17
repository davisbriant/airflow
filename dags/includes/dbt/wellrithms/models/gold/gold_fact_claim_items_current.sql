{{
  config(
    table_type='iceberg',
    unique_key='item_key',
    partitioned_by=['claimant_id'],
    format='parquet'
  )
}}

with claim_items_current as (

  select * from {{ ref('silver_test_data_current') }}

),

final as (

  select 
  item_key,
  claimant_id,
  claim_item_id,
  received_date,
  year(received_date) as received_year,
  month(received_date) as received_month,
  charge_amount,
  allowed_amount,
  diagnostic_code_1,
  diagnostic_code_2,
  diagnostic_code_3,
  diagnostic_code_4,
  diagnostic_code_5,
  claim_code,
  units,
  oi_in_network
  from 
  claim_items_current

)

select 
* 
from 
final