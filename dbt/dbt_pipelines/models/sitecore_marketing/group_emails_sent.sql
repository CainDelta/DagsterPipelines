---group emails by day, count number of emails sent on day for campaign,client key. This will remove the problem of having multiple emails and not knowing which one was sent


{{ config(materialized='table', schema = 'Sitecore',alias='Grouped_Emails') }}

select [Tracking Type Description],[Campaign ID],[Message ID],cast([Tracking Datetime] as date) as [Tracking Date],[BBIS Client Key],[Campaign Display Label], count(*) as [Emails Sent on Day]
from {{ref('emails_sent')}}
group by [Tracking Type Description],[Campaign ID],[Message ID],cast([Tracking Datetime] as date),[BBIS Client Key],[Campaign Display Label]
