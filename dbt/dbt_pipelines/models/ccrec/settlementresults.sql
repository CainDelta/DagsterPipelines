
{{ config(materialized='view', alias='SettlementResults') }}

SELECT
	Results.[SettlementResultsPK]
,	[TransactionDate]=CAST(Results.[TransactionDate] AS DATE)
,	Results.[Amount]
,	[BBIS Risk Key]		= COALESCE(mcrisk.rrpkey,qmast.qmrkey)
,	[BBIS Renewal Sequence] = COALESCE(TRANS.RenewalSequence,0)
,	[BBIS Amendment Sequence] = COALESCE(TRANS.AdjustmentSequence,0)
,	[BBIS Client Key]	= COALESCE(mcrisk.rrckey,qmast.qmckey)
,	[Amount_WithSign]	= CASE WHEN results.Refund = 1 THEN -results.Amount ELSE results.Amount END
,	[Policy Number] = COALESCE([Policy Number],cast(QMAST.[QMPKEY] as varchar(20)))
,	TransactionLogPK

FROM		{{ ref('combined_settlement_results') }}					results
LEFT JOIN		{{ ref('combined_transaction_logs') }}						TRANS
ON			TRANS.TransactionLogPK = RESULTS.Reference
LEFT JOIN	Bennetts_Snapshot.dbo.MCRISK							MCRISK
on			TRANS.RiskNumber = MCRISK.RRPKEY
LEFT JOIN	Bennetts_Snapshot.dbo.MCQMAST						QMAST
ON			TRANS.QuoteFK = QMAST.QMPKEY
LEFT JOIN	Bennetts_MI.Policies.V_Master_Policies		POLICIES_MI
ON			COALESCE(mcrisk.rrpkey,qmast.qmrkey)	= POLICIES_MI.[BBIS Risk Key]
AND			COALESCE(TRANS.RenewalSequence,0)		= POLICIES_MI.[BBIS Renewal Sequence]
AND			COALESCE(TRANS.AdjustmentSequence,0)	= POLICIES_MI.[BBIS Amendment Sequence]
WHERE results.TransactionDate BETWEEN dateadd(dd,-35,getdate()) AND getdate()   ---- Past 35 days of data
