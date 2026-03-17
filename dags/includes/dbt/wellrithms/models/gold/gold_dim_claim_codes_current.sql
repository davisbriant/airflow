{{
  config(
    table_type='iceberg',
    unique_key='primary_key',
    partitioned_by=['claim_code'],
    format='parquet'
  )
}}

with claim_items_current as (

  select * from {{ ref('silver_test_data_current') }}

),

final as (

  select 
  {{ dbt_utils.generate_surrogate_key(['claim_code','claim_code_modifier','claim_code_modifier_2']) }} as primary_key,
  claim_code,
  revenue_code_procedure_description,
  claim_code_modifier,
  claim_code_modifier_2
  from 
  claim_items_current
  group by 
  claim_code,
  revenue_code_procedure_description,
  claim_code_modifier,
  claim_code_modifier_2

)

select 
* 
from 
final