---Add grouped sent emails back into Events Dataset while excluding ungrouped sent emails


{{ config(materialized='table', schema = 'Sitecore',alias='Grouped_Events') }}

select a.*, [Emails Sent on Day]  = null  from {{ref('create_events_table')}} a
where [Tracking Type Description] <> 'Email Sent'
and [Campaign Display Label] = 'R-5 Email'
union all
select
        c.[Event ID],a.[BBIS Client Key],a.[Campaign ID]
        ,a.[Campaign Display Label],a.[Message ID],a.[Tracking Type Description]
		    ,c.[Tracking Datetime],a.[Emails Sent on Day]

        from {{ref('group_emails_sent')}} a
left join
(select [Event ID],[Message ID],[BBIS Client Key],max([Tracking Datetime]) as [Tracking Datetime]
from  {{ref('emails_sent')}}
group by [Event ID],[Message ID],[BBIS Client Key],[Campaign Display Label]
having [Campaign Display Label] = 'R-5 Email') c
on c.[BBIS Client Key] = a.[BBIS Client Key]
and c.[Event ID] = c.[Event ID]
and c.[Message ID] = a.[Message ID]
and cast(c.[Tracking Datetime] as date)  = a.[Tracking Date]
where [Campaign Display Label] = 'R-5 Email'
union all
select a.*, [Emails Sent on Day]  = null  from {{ref('create_events_table')}} a
where [Tracking Type Description] <> 'Email Sent'
and  [Campaign Display Label] = 'RP-5 Email'
union all
select
        c.[Event ID],a.[BBIS Client Key],a.[Campaign ID]
        ,a.[Campaign Display Label],a.[Message ID],a.[Tracking Type Description]
		    ,c.[Tracking Datetime],a.[Emails Sent on Day]

        from {{ref('group_emails_sent')}} a
left join
(select [Event ID],[Message ID],[BBIS Client Key],max([Tracking Datetime]) as [Tracking Datetime]
from  {{ref('emails_sent')}}
group by [Event ID],[Message ID],[BBIS Client Key],[Campaign Display Label]
having [Campaign Display Label] = 'RP-5 Email') c
on c.[BBIS Client Key] = a.[BBIS Client Key]
and c.[Event ID] = c.[Event ID]
and c.[Message ID] = a.[Message ID]
and cast(c.[Tracking Datetime] as date)  = a.[Tracking Date]
where [Campaign Display Label] = 'RP-5 Email'
union all
select a.*, [Emails Sent on Day]  = null  from {{ref('create_events_table')}} a
where [Tracking Type Description] <> 'Email Sent'
and  [Campaign Display Label] = 'R-14 Email'
union all
select
        c.[Event ID],a.[BBIS Client Key],a.[Campaign ID]
        ,a.[Campaign Display Label],a.[Message ID],a.[Tracking Type Description]
		    ,c.[Tracking Datetime],a.[Emails Sent on Day]

        from {{ref('group_emails_sent')}} a
left join
(select [Event ID],[Message ID],[BBIS Client Key],max([Tracking Datetime]) as [Tracking Datetime]
from  {{ref('emails_sent')}}
group by [Event ID],[Message ID],[BBIS Client Key],[Campaign Display Label]
having [Campaign Display Label] = 'R-14 Email') c
on c.[BBIS Client Key] = a.[BBIS Client Key]
and c.[Event ID] = c.[Event ID]
and c.[Message ID] = a.[Message ID]
and cast(c.[Tracking Datetime] as date)  = a.[Tracking Date]
where [Campaign Display Label] = 'R-14 Email'
union all
select a.*, [Emails Sent on Day]  = null  from {{ref('create_events_table')}} a
where [Tracking Type Description] <> 'Email Sent'
and  [Campaign Display Label] = 'R-40 Email'
union all
select
        c.[Event ID],a.[BBIS Client Key],a.[Campaign ID]
        ,a.[Campaign Display Label],a.[Message ID],a.[Tracking Type Description]
		    ,c.[Tracking Datetime],a.[Emails Sent on Day]

        from {{ref('group_emails_sent')}} a
left join
(select [Event ID],[Message ID],[BBIS Client Key],max([Tracking Datetime]) as [Tracking Datetime]
from  {{ref('emails_sent')}}
group by [Event ID],[Message ID],[BBIS Client Key],[Campaign Display Label]
having [Campaign Display Label] = 'R-40 Email') c
on c.[BBIS Client Key] = a.[BBIS Client Key]
and c.[Event ID] = c.[Event ID]
and c.[Message ID] = a.[Message ID]
and cast(c.[Tracking Datetime] as date)  = a.[Tracking Date]
where [Campaign Display Label] = 'R-40 Email'
