{{
  config(
    materialized='table'
  )
}}

SELECT * FROM {{ source('arkestral', 'googleads_airflow_reports_ads')}}