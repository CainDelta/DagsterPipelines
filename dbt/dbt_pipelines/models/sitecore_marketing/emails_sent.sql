

{{ config(materialized='table', schema = 'Sitecore',alias='Emails_Sent') }}

select *
From {{ref('create_events_table')}}
where [Tracking Type Description] = 'Email Sent'
