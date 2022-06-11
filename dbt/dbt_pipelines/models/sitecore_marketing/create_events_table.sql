

{{ config(materialized='table', schema = 'Sitecore',alias='Master_Events') }}


with events_data as (

SELECT *
FROM   (SELECT
               cast([Event ID] as nvarchar(40)) as [Event ID],
               cast([BBIS Client Key] as nvarchar(10)) as [BBIS Client Key],
               [Campaign ID],
               [Campaign Display Label],
               cast([Message ID] as nvarchar(40)) as [Message ID],
               [Tracking Type Description],
               [Tracking Datetime]
        --FROM   sitecore.v_events
        FROM Testing.T_Sitecore_Events
        WHERE  [bbis client key] NOT LIKE '%ANDY%') b
GROUP  BY      [Event ID],
               [Tracking Type Description],
               [Campaign ID],
               [Message ID],
               [Tracking Datetime],
               [BBIS Client Key] ,
               [Campaign Display Label])

select * from events_data
