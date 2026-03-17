{{
  config(
    materialized = 'table',
    table_type = 'iceberg',
    unique_key = 'pkey',
    partition_by = ['accountid'],
  )
}}

select 
md5(to_utf8(concat(a.personid, a.accountid, a.adid, json_extract_scalar(a.json_payload, '$.adGroupAd.status')))) as pkey,
"$path" as sourcefile,
a.personid,
a.accountid as accountid,
a.adid as adid,
json_extract_scalar(a.json_payload, '$.adGroupAd.status') as status,
json_extract_scalar(a.json_payload, '$.adGroupAd.ad.type') as type,
case
when json_extract_scalar(a.json_payload, '$.adGroupAd.ad.type') = 'EXPANDED_TEXT_AD'
then
json_format(json_extract(json_payload, '$.adGroupAd.ad.expandedTextAd'))
else
'unknown'
end as details,
json_format(json_extract(json_payload, '$.adGroupAd.ad.finalUrls')) as finalurls,
json_extract_scalar(json_payload, '$.adGroupAd.ad.addedByGoogleAds') as addedbygoogleads,
json_extract_scalar(json_payload, '$.adGroupAd.policySummary.approvalStatus') as approvalstatus,
element_at(split(json_extract_scalar(json_payload, '$.adGroupAd.adGroup'),'/'),-1) as adgroupid
from
{{ ref('stg_googleads_objects_ads') }} a
WHERE
a.json_payload NOT LIKE '%no data%'
GROUP BY
1,2,3,4,5,6,7,8,9,10,11,12
