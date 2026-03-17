{{
  config(
    materialized='table',
    table_type = 'iceberg',
    unique_key = 'pkey',
    partition_by = ['date'],
  )
}}

select 
md5(to_utf8(concat(json_extract_scalar(a.json_payload, '$.segments.date'), a.personid, a.accountid, a.adid, json_extract_scalar(a.json_payload, '$.segments.device'), json_extract_scalar(a.json_payload, '$.segments.adNetworkType')))) as pkey,
"$path" as sourcefile,
a.personid,
a.accountid,
json_extract_scalar(a.json_payload, '$.campaign.id') as campaignid,
element_at(split(json_extract_scalar(a.json_payload, '$.adGroup.baseAdGroup'),'/'),-1) as adgroupid,
json_format(json_extract(a.json_payload, '$.metrics.interactionEventTypes')) as interactioneventtypes,
json_extract_scalar(a.json_payload, '$.segments.date') as "date",
json_extract_scalar(a.json_payload, '$.segments.device') as "device",
json_extract_scalar(a.json_payload, '$.segments.adNetworkType') as "adnetworktype",
a.adid as adid,
sum(cast(json_extract_scalar(a.json_payload, '$.metrics.impressions') as bigint)) AS "impressions",
sum(cast(json_extract_scalar(a.json_payload, '$.metrics.clicks') as bigint)) AS "clicks",
sum(cast(json_extract_scalar(a.json_payload, '$.metrics.costMicros') as double))/1000000 AS "cost",
sum(cast(json_extract_scalar(a.json_payload, '$.metrics.topImpressionPercentage') as real)) AS "topimpressionpercentage",
sum(cast(json_extract_scalar(a.json_payload, '$.metrics.videoviews') as double)) AS "videoviews",
sum(cast(json_extract_scalar(a.json_payload, '$.metrics.viewThroughConversions') as double)) AS "viewthroughconversion",
sum(cast(json_extract_scalar(a.json_payload, '$.metrics.conversionsValue') as double)) AS "conversionsValue",
sum(cast(json_extract_scalar(a.json_payload, '$.metrics.conversions') as double)) AS "conversions",
sum(cast(json_extract_scalar(a.json_payload, '$.metrics.currentModelAttributedConversion') as double)) AS "currentmodelattributedconversions",
sum(cast(json_extract_scalar(a.json_payload, '$.metrics.currentModelAttributedConversionValue') as double)) AS "currentmodelattributedconversionsvalue",
sum(cast(json_extract_scalar(a.json_payload, '$.metrics.engagements') as double)) AS "engagements",
sum(cast(json_extract_scalar(a.json_payload, '$.metrics.absoluteTopImpressionPercentage') as double)) AS "absolutetopimpressionpercentage",
sum(cast(json_extract_scalar(a.json_payload, '$.metrics.activeViewImpressions') as double)) AS "activeviewimpressions",
sum(cast(json_extract_scalar(a.json_payload, '$.metrics.activeViewMeasurability') as double)) AS "activeviewmeasurability",
sum(cast(json_extract_scalar(a.json_payload, '$.metrics.activeViewMeasurableCostMicros') as double)) AS "activeviewmeasurablecostmicros",
sum(cast(json_extract_scalar(a.json_payload, '$.metrics.activeViewMeasurabieImpressions') as double)) AS "activeviewmeasurableimpressions",
sum(cast(json_extract_scalar(a.json_payload, '$.metrics.allConversionsValue') as double)) AS "allconversionsvalue",
sum(cast(json_extract_scalar(a.json_payload, '$.metrics.allConversions') as double)) AS "allconversions",
sum(cast(json_extract_scalar(a.json_payload, '$.metrics.gmailForwards') as double)) AS "gmailforwards",
sum(cast(json_extract_scalar(a.json_payload, '$.metrics.gmailSaves') as double)) AS "gmailsaves",
sum(cast(json_extract_scalar(a.json_payload, '$.metrics.gmailSecondaryClicks') as double)) AS "gmailseconfaryclicks",
sum(cast(json_extract_scalar(a.json_payload, '$.metrics.interactions') as double)) AS "interactions"
from
{{ ref('stg_googleads_reports_ads') }} a
WHERE
a.json_payload NOT LIKE '%no data%'
GROUP BY
1,2,3,4,5,6,7,8,9,10,11

