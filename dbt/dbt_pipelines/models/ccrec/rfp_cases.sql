

{{ config(materialized='view', alias='RFP_CASES') }}

select		TransactionReference,CorrelationId,D.[ClientId]
from		Bennetts_Snapshot.Payment.audit					A	with(nolock)
left join	bennetts_mi.misc.T_Transactions			C	with(nolock)
on			A.[CorrelationId] = C.TransactionID
left join	Bennetts_Snapshot.dbo.ExternalClientMapping		D	with(nolock)
ON			C.[CustomerKey]		=	D.[ExternalClientMappingId]
where		TransactionReference in
(	select		cast([TransactionLogPK] as varchar(100))
	from			{{ ref('settlementresults') }}					 with(nolock)
	where		[BBIS Risk Key] = '0'
)	AND [ApplicationName] = 'MyAccount'
