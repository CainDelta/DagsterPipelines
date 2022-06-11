

{{ config(materialized='table', schema = 'Sitecore',alias='summary_table') }}

select

a.[BBIS Client Key],b.[BBIS Risk Key],b.[BBIS Renewal Sequence],b.[BBIS Amendment Sequence],b.[Transaction Loaded Datetime],b.[Transaction Effective Date],b.[Transaction Expiry Date],
[Policy Expiry Date],[PH Opt In Email (New)],[Quote Channel],b.[Transaction Type] ,
b.[Policy Passive Renewal],b.[Policy Currently Passive Blocked],b.[Policy Passive Block Date],b.[Transaction Passive Renewal Detail],b.[Transaction Operator ID],
a.[Campaign Display Label],a.[Campaign Type],a.[Communication ID],a.[Communication Source],a.[Date Delivered] as [Date Delivered R-40],
a.[Date First Opened] as [Date First Opened R-40], a.[Date First Click] as [Date First Click R-40], a.[Count Opened] as [Count Opened R-40],a.[Count Clicks] as [Count Clicks R-40]
,r14.[Date Delivered R-14],r14.[Date First Opened R-14],r14.[Date First Click R-14],r14.[Count Opened R-14],r14.[Count Clicks R-14],
r5.[Date Delivered R-5],r5.[Date First Opened R-5],r5.[Date First Click R-5],r5.[Count Opened R-5],r5.[Count Clicks R-5],
rp5.[Date Delivered RP-5],rp5.[Date First Opened RP-5],rp5.[Date First Click RP-5],rp5.[Count Opened RP-5],rp5.[Count Clicks RP-5]

From {{ref('pre_summary_table')}} a
left join (select [Policy Number],[BBIS Client Key] as bck,[BBIS Risk Key],[BBIS Amendment Sequence],[BBIS Renewal Sequence],[Transaction Loaded Datetime],[Transaction Effective Date],[Transaction Expiry Date],
	[Policy Expiry Date],[PH Opt In Email (New)],[Quote Channel],[Transaction Type] ,[Policy Passive Renewal],[Policy Currently Passive Blocked],[Policy Passive Block Date],[Transaction Passive Renewal Detail],[Transaction Operator ID]
			from Policies.T_Master_Policies
			where [Transaction Type] like '%Renewal Invite%') b
on a.[BBIS Client Key] = b.bck
and DATEDIFF(day,cast(a.[Date Delivered] + 40 as date), cast(b.[Transaction Effective Date] as date)) >= -2 and DATEDIFF(day,cast(a.[Date Delivered] + 40 as date), cast(b.[Transaction Effective Date] as date)) <= 1
left join
(select [Date Delivered] as [Date Delivered R-5],[Date First Opened] as [Date First Opened R-5], [Date First Click] as [Date First Click R-5],
a.[Count Opened] as [Count Opened R-5],a.[Count Clicks] as [Count Clicks R-5] ,
[BBIS Client Key],b.[Policy Number],b.[Transaction Effective Date]
from {{ref('pre_summary_table')}} a
left join (select [Policy Number],[BBIS Client Key] as bck,[BBIS Risk Key],[BBIS Amendment Sequence],[BBIS Renewal Sequence],[Transaction Effective Date],[Transaction Expiry Date]
			from Policies.T_Master_Policies
			where [Transaction Type] like '%Renewal Invite%') b
on a.[BBIS Client Key] = b.bck
and DATEDIFF(day,cast(a.[Date Delivered] + 5 as date), cast(b.[Transaction Effective Date] as date)) >= -2 and
DATEDIFF(day,cast(a.[Date Delivered] + 5 as date), cast(b.[Transaction Effective Date] as date)) <= 1
where [Campaign Display Label] = 'R-5 Email') r5
on a.[BBIS Client Key] = r5.[BBIS Client Key]
and b.[Transaction Effective Date] = r5.[Transaction Effective Date]
left join
(select [Date Delivered] as [Date Delivered R-14],[Date First Opened] as [Date First Opened R-14], [Date First Click] as [Date First Click R-14],
a.[Count Opened] as [Count Opened R-14],a.[Count Clicks] as [Count Clicks R-14] ,
[BBIS Client Key],b.[Policy Number],[Transaction Effective Date]
from {{ref('pre_summary_table')}} a
left join (select [Policy Number],[BBIS Client Key] as bck,[BBIS Risk Key],[BBIS Amendment Sequence],[BBIS Renewal Sequence],[Transaction Effective Date],[Transaction Expiry Date]
			from Policies.T_Master_Policies
			where [Transaction Type] like '%Renewal Invite%') b
on a.[BBIS Client Key] = b.bck
and DATEDIFF(day,cast(a.[Date Delivered] + 14 as date), cast(b.[Transaction Effective Date] as date)) >= -2 and
DATEDIFF(day,cast(a.[Date Delivered] + 14 as date), cast(b.[Transaction Effective Date] as date)) <= 1
where [Campaign Display Label] = 'R-14 Email') r14
on a.[BBIS Client Key] = r14.[BBIS Client Key]
and b.[Transaction Effective Date] = r14.[Transaction Effective Date]
left join
(select [Date Delivered] as [Date Delivered RP-5],[Date First Opened] as [Date First Opened RP-5], [Date First Click] as [Date First Click RP-5],
a.[Count Opened] as [Count Opened RP-5],a.[Count Clicks] as [Count Clicks RP-5] ,
[BBIS Client Key],b.[Policy Number],b.[Transaction Effective Date]
from {{ref('pre_summary_table')}} a
left join (select [Policy Number],[BBIS Client Key] as bck,[BBIS Risk Key],[BBIS Amendment Sequence],[BBIS Renewal Sequence],[Transaction Effective Date],[Transaction Expiry Date]
			from Policies.T_Master_Policies
			where [Transaction Type] like '%Renewal Invite%') b
on a.[BBIS Client Key] = b.bck
and DATEDIFF(day,cast(a.[Date Delivered] + 5 as date), cast(b.[Transaction Effective Date] as date)) >= -2 and
DATEDIFF(day,cast(a.[Date Delivered] + 5 as date), cast(b.[Transaction Effective Date] as date)) <= 1
where [Campaign Display Label] = 'RP-5 Email') rp5
on a.[BBIS Client Key] = rp5.[BBIS Client Key]
and b.[Transaction Effective Date] = rp5.[Transaction Effective Date]
where a.[Campaign Display Label] = 'R-40 Email'
and b.[BBIS Risk Key] is not null
