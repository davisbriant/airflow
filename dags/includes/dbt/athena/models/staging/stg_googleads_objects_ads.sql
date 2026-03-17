{{
  config(
    materialized='table'
  )
}}

SELECT * FROM {{ source('arkestral', 'googleads_airflow_objects_ads')}}