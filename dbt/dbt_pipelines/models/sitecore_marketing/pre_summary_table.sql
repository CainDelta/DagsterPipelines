

{{ config(materialized='table', schema = 'Sitecore',alias='pre_summary_table') }}

SELECT
        [Campaign Display Label]
,		[Campaign Type]											   =CAST(NULL AS VARCHAR(100))
,		[BBIS Client Key]										   =[BBIS Client Key]
--,		[Client First Name]										   =[Sitecore First Name]
--,		[Client Surname]										   =[Sitecore Surname]
--,		[Communication Address]									   =[Sitecore Email Address]
,		[Communication ID]										   =[Campaign ID]
,		[Communication Source]									   ='Sitecore'
,		[Communication Type]									   ='Email'
,		[Unique Email ID]										   = MIN(EmailID)
,		[Count Emails Sent]										   = SUM(cast([Emails Sent on Day] as int))
--,		[Extract Date]											   =MIN([Loaded Datetime])

,		[Date Delivered]										   =cast(MIN(CASE WHEN [Tracking Type Description] = 'Email Sent' then [Tracking Datetime] else NULL end)as datetime)
,		[Date First Opened]										   =cast(MIN(CASE WHEN [Tracking Type Description] = 'Open' then [Tracking Datetime] else NULL end)as datetime)
,		[Date Last Opened]										   =cast(MAX(CASE WHEN [Tracking Type Description] = 'Open' then [Tracking Datetime] else NULL end)as datetime )
,		[Count Opened]											   =COUNT(CASE WHEN [Tracking Type Description] = 'Open' then [Tracking Datetime] else NULL end)
,		[Count Opened Deduped]									   =MAX(CASE WHEN [Tracking Type Description] = 'Open' then 1 else NULL end)
,		[Date First Click]										   =cast(MIN(CASE WHEN [Tracking Type Description] = 'Email Click' then [Tracking Datetime] else NULL end)as datetime)
,		[Date Last Click]										   =cast(MAX(CASE WHEN [Tracking Type Description] = 'Email Click' then [Tracking Datetime] else NULL end)as datetime)
,		[Count Clicks]											   =COUNT(CASE WHEN [Tracking Type Description] = 'Email Click' then [Tracking Datetime] else NULL end)
,		[Count Unique Clicks]									   =COUNT(distinct CASE WHEN [Tracking Type Description] = 'Email Click' then [Tracking Datetime] else NULL end)
--,		[Unsubscribe Source]									   =CAST(NULL AS VARCHAR(100))
--,		[Click Label: First]									   =max(CASE WHEN [Tracking Type Description] = 'Email Click' AND [Click Sequence] = '1' THEN [Tracking URL] else null end)
--,		[Click URL:	First]										   =max(CASE WHEN [Tracking Type Description] = 'Email Click' AND [Click Sequence] = '1' THEN [Tracking URL] else null end)
--,		[Click Label: Last]										   =max(CASE WHEN [Tracking Type Description] = 'Email Click' AND [Click Sequence REV] = '1' THEN [Tracking URL] else null end)
--,		[Click URL:	Last]										   =max(CASE WHEN [Tracking Type Description] = 'Email Click' AND [Click Sequence REV] = '1' THEN [Tracking URL] else null end)



  --into #Pre
  from Sitecore.EmailEvents
  group by
   [Campaign Display Label]
  ,[BBIS Client Key]
  ,[Campaign ID]
  ,EmailID
