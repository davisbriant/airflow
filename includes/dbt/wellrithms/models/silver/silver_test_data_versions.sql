{{
  config(
    table_type='iceberg',
    unique_key='primary_key',
    partitioned_by=['service_provider'],
    format='parquet'
  )
}}

with raw_data as (

  select * from {{ ref('bronze_test_data') }}

),

renamed as (

  SELECT 
  _path,
  converted_at,
  try_cast(JSON_EXTRACT_SCALAR(json_payload, '$.CLAIMANT_ID') as int) AS claimant_id,
  try_cast(JSON_EXTRACT_SCALAR(json_payload, '$.CLAIM_ITEM_ID') as int) AS claim_item_id,
  JSON_EXTRACT_SCALAR(json_payload, '$.TYPE') AS type,
  try_cast(date_parse(JSON_EXTRACT_SCALAR(json_payload, '$.RECEIVED_DATE'), '%m/%d/%y') as date) AS received_date,
  try_cast(JSON_EXTRACT_SCALAR(json_payload, '$.CHARGE_AMT')as decimal) AS charge_amount,
  try_cast(JSON_EXTRACT_SCALAR(json_payload, '$.ALLOWED_AMT')as decimal) AS allowed_amount,
  JSON_EXTRACT_SCALAR(json_payload, '$.DIAG_CODE_1') AS diagnostic_code_1,
  JSON_EXTRACT_SCALAR(json_payload, '$.DIAG_CODE_2') AS diagnostic_code_2,
  JSON_EXTRACT_SCALAR(json_payload, '$.DIAG_CODE_3') AS diagnostic_code_3,
  JSON_EXTRACT_SCALAR(json_payload, '$.DIAG_CODE_4') AS diagnostic_code_4,
  JSON_EXTRACT_SCALAR(json_payload, '$.DIAG_CODE_5') AS diagnostic_code_5,
  JSON_EXTRACT_SCALAR(json_payload, '$.CLAIM_CODE') AS claim_code,
  JSON_EXTRACT_SCALAR(json_payload, '$["RevCode/PROCEDURE_DESCRIPTION"]') AS revenue_code_procedure_description,
  JSON_EXTRACT_SCALAR(json_payload, '$.CLAIM_CODE_MODIFIER') AS claim_code_modifier,
  JSON_EXTRACT_SCALAR(json_payload, '$.CLAIM_CODE_MODIFIER_2') AS claim_code_modifier_2,
  try_cast(JSON_EXTRACT_SCALAR(json_payload, '$.units') as int) AS units,
  JSON_EXTRACT_SCALAR(json_payload, '$.OI_IN_NETWORK') AS oi_in_network,
  JSON_EXTRACT_SCALAR(json_payload, '$.SERVICE_PROVIDER') AS service_provider,
  JSON_EXTRACT_SCALAR(json_payload, '$.SERVICE_ADDRESSS_3') AS service_address_3,
  JSON_EXTRACT_SCALAR(json_payload, '$.SERVICE_ADDRESSS_2') AS service_address_2
  FROM raw_data

),

final as (

  select 
  {{ dbt_utils.generate_surrogate_key(['service_provider', 'claimant_id', 'claim_item_id', 'converted_at']) }} as primary_key,
  {{ dbt_utils.generate_surrogate_key(['service_provider', 'claimant_id', 'claim_item_id']) }} as item_key,
  row_number() over (partition by service_provider, claimant_id, claim_item_id order by converted_at asc) as item_key_version,
  * 
  from 
  renamed

)

select 
* 
from 
final