{{
  config(
    materialized='table'
  )
}}

SELECT * FROM {{ source('arkestral', 'googleads_matchtables_ads')}}