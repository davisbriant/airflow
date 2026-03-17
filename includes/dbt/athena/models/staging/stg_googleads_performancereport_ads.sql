{{
  config(
    materialized='table'
  )
}}

SELECT * FROM {{ source('arkestral', 'googleads_basicperformancereport_ads')}}