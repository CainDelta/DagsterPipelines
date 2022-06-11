

{{ config(materialized='view', alias='CreditCard_Rec_SLTRANS') }}


WITH CreditCard_Rec_SLTRANS AS (

SELECT
   [Reconciled] = case when isnull([Settlement_Amount_Full],0) + isnull([Braintree_Amount_Full],0)  <> isnull(STVAL_Full,0) then 0 else 1 end
,		[Loaded Datetime]				= getdate()
,		[BBIS Client Key]				= COALESCE(A.[BBIS Client Key],b.CRPKEY)
,		[PH Full Name]					= CAST(CRTITL AS VARCHAR(10)) + ' ' + CRCHRN + ' ' + CRSURN


,		[Settlement Value 1]			= CONVERT(NVARCHAR(10),A_DATES.[1],103) + ': ' +char(10)+ CAST(A.[1] AS NVARCHAR(10)) + char(10) + '( ' + CAST(A_POLICY.[1] AS NVARCHAR(10)) + ' )'
,		[Settlement Value 2]			= CONVERT(NVARCHAR(10),A_DATES.[2],103) + ': ' +char(10)+ CAST(A.[2] AS NVARCHAR(10)) + char(10) + '( ' + CAST(A_POLICY.[2] AS NVARCHAR(10)) + ' )'
,		[Settlement Value 3]			= CONVERT(NVARCHAR(10),A_DATES.[3],103) + ': ' +char(10)+ CAST(A.[3] AS NVARCHAR(10)) + char(10) + '( ' + CAST(A_POLICY.[3] AS NVARCHAR(10)) + ' )'
,		[Settlement Value 4]			= CONVERT(NVARCHAR(10),A_DATES.[4],103) + ': ' +char(10)+ CAST(A.[4] AS NVARCHAR(10)) + char(10) + '( ' + CAST(A_POLICY.[4] AS NVARCHAR(10)) + ' )'
,		[Settlement Value 5]			= CONVERT(NVARCHAR(10),A_DATES.[5],103) + ': ' +char(10)+ CAST(A.[5] AS NVARCHAR(10)) + char(10) + '( ' + CAST(A_POLICY.[5] AS NVARCHAR(10)) + ' )'

,		[Settlement Value 6]			= CONVERT(NVARCHAR(10),A_DATES.[6],103) + ': ' +char(10)+ CAST(A.[6] AS NVARCHAR(10)) + char(10) + '( ' + CAST(A_POLICY.[6] AS NVARCHAR(10)) + ' )'
,		[Settlement Value 7]			= CONVERT(NVARCHAR(10),A_DATES.[7],103) + ': ' +char(10)+ CAST(A.[7] AS NVARCHAR(10)) + char(10) + '( ' + CAST(A_POLICY.[7] AS NVARCHAR(10)) + ' )'
,		[Settlement Value 8]			= CONVERT(NVARCHAR(10),A_DATES.[8],103) + ': ' +char(10)+ CAST(A.[8] AS NVARCHAR(10)) + char(10) + '( ' + CAST(A_POLICY.[8] AS NVARCHAR(10)) + ' )'
,		[Settlement Value 9]			= CONVERT(NVARCHAR(10),A_DATES.[9],103) + ': ' +char(10)+ CAST(A.[9] AS NVARCHAR(10)) + char(10) + '( ' + CAST(A_POLICY.[9] AS NVARCHAR(10)) + ' )'
,		[Settlement Value 10]			= CONVERT(NVARCHAR(10),A_DATES.[10],103) + ': ' +char(10)+ CAST(A.[10] AS NVARCHAR(10)) + char(10) + '( ' + CAST(A_POLICY.[10] AS NVARCHAR(10)) + ' )'

,		[Ledger Value 1]				= CONVERT(NVARCHAR(10),B_DATES.[1],103) + ': ' +char(10)+ CAST(B.[1] AS NVARCHAR(10)) + char(10) + '( ' + CAST(B_POLICY.[1] AS NVARCHAR(10)) + ' )'
,		[Ledger Value 2]				= CONVERT(NVARCHAR(10),B_DATES.[2],103) + ': ' +char(10)+ CAST(B.[2] AS NVARCHAR(10)) + char(10) + '( ' + CAST(B_POLICY.[2] AS NVARCHAR(10)) + ' )'
,		[Ledger Value 3]				= CONVERT(NVARCHAR(10),B_DATES.[3],103) + ': ' +char(10)+ CAST(B.[3] AS NVARCHAR(10)) + char(10) + '( ' + CAST(B_POLICY.[3] AS NVARCHAR(10)) + ' )'
,		[Ledger Value 4]				= CONVERT(NVARCHAR(10),B_DATES.[4],103) + ': ' +char(10)+ CAST(B.[4] AS NVARCHAR(10)) + char(10) + '( ' + CAST(B_POLICY.[4] AS NVARCHAR(10)) + ' )'
,		[Ledger Value 5]				= CONVERT(NVARCHAR(10),B_DATES.[5],103) + ': ' +char(10)+ CAST(B.[5] AS NVARCHAR(10)) + char(10) + '( ' + CAST(B_POLICY.[5] AS NVARCHAR(10)) + ' )'

,		[Ledger Value 6]				= CONVERT(NVARCHAR(10),B_DATES.[6],103) + ': ' +char(10)+ CAST(B.[6] AS NVARCHAR(10)) + char(10) + '( ' + CAST(B_POLICY.[6] AS NVARCHAR(10)) + ' )'
,		[Ledger Value 7]				= CONVERT(NVARCHAR(10),B_DATES.[7],103) + ': ' +char(10)+ CAST(B.[7] AS NVARCHAR(10)) + char(10) + '( ' + CAST(B_POLICY.[7] AS NVARCHAR(10)) + ' )'
,		[Ledger Value 8]				= CONVERT(NVARCHAR(10),B_DATES.[8],103) + ': ' +char(10)+ CAST(B.[8] AS NVARCHAR(10)) + char(10) + '( ' + CAST(B_POLICY.[8] AS NVARCHAR(10)) + ' )'
,		[Ledger Value 9]				= CONVERT(NVARCHAR(10),B_DATES.[9],103) + ': ' +char(10)+ CAST(B.[9] AS NVARCHAR(10)) + char(10) + '( ' + CAST(B_POLICY.[9] AS NVARCHAR(10)) + ' )'
,		[Ledger Value 10]				= CONVERT(NVARCHAR(10),B_DATES.[10],103) + ': ' +char(10)+ CAST(B.[10] AS NVARCHAR(10)) + char(10) + '( ' + CAST(B_POLICY.[10] AS NVARCHAR(10)) + ' )'

,		[Ledger Value 11]				= CONVERT(NVARCHAR(10),B_DATES.[11],103) + ': ' +char(10)+ CAST(B.[11] AS NVARCHAR(10)) + char(10) + '( ' + CAST(B_POLICY.[11] AS NVARCHAR(10)) + ' )'
,		[Ledger Value 12]				= CONVERT(NVARCHAR(10),B_DATES.[12],103) + ': ' +char(10)+ CAST(B.[12] AS NVARCHAR(10)) + char(10) + '( ' + CAST(B_POLICY.[12] AS NVARCHAR(10)) + ' )'
,		[Ledger Value 13]				= CONVERT(NVARCHAR(10),B_DATES.[13],103) + ': ' +char(10)+ CAST(B.[13] AS NVARCHAR(10)) + char(10) + '( ' + CAST(B_POLICY.[13] AS NVARCHAR(10)) + ' )'
,		[Ledger Value 14]				= CONVERT(NVARCHAR(10),B_DATES.[14],103) + ': ' +char(10)+ CAST(B.[14] AS NVARCHAR(10)) + char(10) + '( ' + CAST(B_POLICY.[14] AS NVARCHAR(10)) + ' )'
,		[Ledger Value 15]				= CONVERT(NVARCHAR(10),B_DATES.[15],103) + ': ' +char(10)+ CAST(B.[15] AS NVARCHAR(10)) + char(10) + '( ' + CAST(B_POLICY.[15] AS NVARCHAR(10)) + ' )'

,    	[Braintree Value 1]				= CONVERT(NVARCHAR(10),B2_DATES.[1],103) + ': ' +char(10)+ CAST(B2.[1] AS NVARCHAR(10)) + char(10) + '( ' + ISNULL(CAST(B2_POLICY.[1] AS NVARCHAR(11)),'No Risk Key') + ' )'
,		[Braintree Value 2]				= CONVERT(NVARCHAR(10),B2_DATES.[2],103) + ': ' +char(10)+ CAST(B2.[2] AS NVARCHAR(10)) + char(10) + '( ' + ISNULL(CAST(B2_POLICY.[2] AS NVARCHAR(11)),'No Risk Key') + ' )'
,		[Braintree Value 3]				= CONVERT(NVARCHAR(10),B2_DATES.[3],103) + ': ' +char(10)+ CAST(B2.[3] AS NVARCHAR(10)) + char(10) + '( ' + ISNULL(CAST(B2_POLICY.[3] AS NVARCHAR(11)),'No Risk Key') + ' )'
,		[Braintree Value 4]				= CONVERT(NVARCHAR(10),B2_DATES.[4],103) + ': ' +char(10)+ CAST(B2.[4] AS NVARCHAR(10)) + char(10) + '( ' + ISNULL(CAST(B2_POLICY.[4] AS NVARCHAR(11)),'No Risk Key') + ' )'
,		[Braintree Value 5]				= CONVERT(NVARCHAR(10),B2_DATES.[5],103) + ': ' +char(10)+ CAST(B2.[5] AS NVARCHAR(10)) + char(10) + '( ' + ISNULL(CAST(B2_POLICY.[5] AS NVARCHAR(11)),'No Risk Key') + ' )'
,		[Braintree Value 6]				= CONVERT(NVARCHAR(10),B2_DATES.[6],103) + ': ' +char(10)+ CAST(B2.[6] AS NVARCHAR(10)) + char(10) + '( ' + ISNULL(CAST(B2_POLICY.[6] AS NVARCHAR(11)),'No Risk Key') + ' )'
,		[Braintree Value 7]				= CONVERT(NVARCHAR(10),B2_DATES.[7],103) + ': ' +char(10)+ CAST(B2.[7] AS NVARCHAR(10)) + char(10) + '( ' + ISNULL(CAST(B2_POLICY.[7] AS NVARCHAR(11)),'No Risk Key') + ' )'
,		[Braintree Value 8]				= CONVERT(NVARCHAR(10),B2_DATES.[8],103) + ': ' +char(10)+ CAST(B2.[8] AS NVARCHAR(10)) + char(10) + '( ' + ISNULL(CAST(B2_POLICY.[8] AS NVARCHAR(11)),'No Risk Key') + ' )'
,		[Braintree Value 9]				= CONVERT(NVARCHAR(10),B2_DATES.[9],103) + ': ' +char(10)+ CAST(B2.[9] AS NVARCHAR(10)) + char(10) + '( ' + ISNULL(CAST(B2_POLICY.[9] AS NVARCHAR(11)),'No Risk Key') + ' )'
,		[Braintree Value 10]			= CONVERT(NVARCHAR(10),B2_DATES.[10],103) + ': '+char(10)+ CAST(B2.[10] AS NVARCHAR(10)) +char(10) + '( ' + ISNULL(CAST(B2_POLICY.[10] AS NVARCHAR(11)),'No Risk Key') + ' )'


,		[Latest Settlement Date]		= CAST(COALESCE(A_DATES.[10],A_DATES.[9],A_DATES.[8],A_DATES.[7],A_DATES.[6],A_DATES.[5],A_DATES.[4],A_DATES.[3],A_DATES.[2],A_DATES.[1]) AS DATETIME)
,		[Latest Ledger Date]			= CAST(COALESCE(B_DATES.[15],B_DATES.[14],B_DATES.[13],B_DATES.[12],B_DATES.[11],B_DATES.[10],B_DATES.[9],B_DATES.[8],B_DATES.[7],B_DATES.[6],B_DATES.[5],B_DATES.[4],B_DATES.[3],B_DATES.[2],B_DATES.[1]) AS DATETIME)
,		[Latest Braintree Date]			= CAST(COALESCE(B2_DATES.[10],B2_DATES.[9],B2_DATES.[8],B2_DATES.[7],B2_DATES.[6],B2_DATES.[5],B2_DATES.[4],B2_DATES.[3],B2_DATES.[2],B2_DATES.[1]) AS DATETIME)


,		[Latest Date]					= CAST(CASE WHEN ISNULL(COALESCE(A_DATES.[10],A_DATES.[9],A_DATES.[8],A_DATES.[7],A_DATES.[6],A_DATES.[5],A_DATES.[4],A_DATES.[3],A_DATES.[2],A_DATES.[1]),'1 January 1900') >= ISNULL(COALESCE(B_DATES.[15],B_DATES.[14],B_DATES.[13],B_DATES.[12],B_DATES.[11],B_DATES.[10],B_DATES.[9],B_DATES.[8],B_DATES.[7],B_DATES.[6],B_DATES.[5],B_DATES.[4],B_DATES.[3],B_DATES.[2],B_DATES.[1]),'1 January 1900') THEN COALESCE(A_DATES.[10],A_DATES.[9],A_DATES.[8],A_DATES.[7],A_DATES.[6],A_DATES.[5],A_DATES.[4],A_DATES.[3],A_DATES.[2],A_DATES.[1]) ELSE COALESCE(B_DATES.[15],B_DATES.[14],B_DATES.[13],B_DATES.[12],B_DATES.[11],B_DATES.[10],B_DATES.[9],B_DATES.[8],B_DATES.[7],B_DATES.[6],B_DATES.[5],B_DATES.[4],B_DATES.[3],B_DATES.[2],B_DATES.[1]) END AS DATETIME)

,		[Settlement Total]				= A.Settlement_Amount_Full
,		[Ledger Total]					= B.STVAL_Full
,		[Braintree Total]				= B2.Braintree_Amount_Full

--,		[Mismatch]						= isnull(B.STVAL_Full,0) - isnull(A.Settlement_Amount_Full,0)
,		[Mismatch]						= isnull(B.STVAL_Full,0) - isnull(A.Settlement_Amount_Full,0) - isnull(B2.[Braintree_Amount_Full],0)


FROM	 {{ ref('settlementresults_pivot') }}		A
LEFT JOIN	{{ ref('settlementresults_date_pivot') }}	A_DATES
ON			A.[BBIS Client Key] = A_DATES.[BBIS Client Key]
LEFT JOIN	{{ ref('settlementresults_policy_pivot') }}	A_POLICY
ON			A.[BBIS Client Key] = A_POLICY.[BBIS Client Key]


FULL JOIN	{{ ref('sltrans_pivot') }}					B					---- FULL Join onto Ledger to ensure any extra non-matched ledger entries are picked up
ON			A.[BBIS Client Key] = B.CRPKEY
LEFT JOIN	{{ ref('sltrans_date_pivot') }}			B_DATES
ON			B.CRPKEY = B_DATES.CRPKEY
LEFT JOIN	{{ ref('sltrans_policy_pivot') }}			B_POLICY
ON			B.CRPKEY = B_POLICY.CRPKEY




FULL JOIN	{{ ref('braintree_data_pivot') }}				B2					---- FULL Join onto Braintree to ensure any extra non-matched ledger entries are picked up
ON			B.CRPKEY = B2.CLIENTID
LEFT JOIN	{{ ref('braintree_data_date_pivot') }}					B2_DATES
ON			B2.CLIENTID = B2_DATES.CLIENTID
LEFT JOIN	{{ ref('braintree_data_policy_pivot') }}				B2_POLICY
ON			B2.CLIENTID = B2_POLICY.CLIENTID


LEFT JOIN	Bennetts_Snapshot.DBO.CLIENT			C
ON			COALESCE(A.[BBIS Client Key],b.CRPKEY) = C.CRPKEY

WHERE

		(	A_DATES.[2] IS NOT NULL OR B_DATES.[1] IS NOT NULL
			OR A_DATES.[1] <> (SELECT MIN(cast(TransactionDate as date)) FROM {{ ref('settlementresults') }})
		)																					---- Remove cases where Settlement is from the earliest possible day - these will always fail due to the previous day's ledger not being included:
AND
	isnull(B.STVAL_Full,0) - isnull(A.Settlement_Amount_Full,0) <>
	ISNULL(CASE WHEN A_DATES.[1] = (SELECT MIN(cast(TransactionDate as date)) FROM {{ ref('settlementresults') }}) THEN -A.[1] END,0)
+	ISNULL(CASE WHEN A_DATES.[2] = (SELECT MIN(cast(TransactionDate as date)) FROM {{ ref('settlementresults') }}) THEN -A.[2] END,0)
+	ISNULL(CASE WHEN A_DATES.[3] = (SELECT MIN(cast(TransactionDate as date)) FROM {{ ref('settlementresults') }}) THEN -A.[3] END,0)
+	ISNULL(CASE WHEN A_DATES.[4] = (SELECT MIN(cast(TransactionDate as date)) FROM {{ ref('settlementresults') }}) THEN -A.[4] END,0)
+	ISNULL(CASE WHEN A_DATES.[5] = (SELECT MIN(cast(TransactionDate as date)) FROM {{ ref('settlementresults') }}) THEN -A.[5] END,0)
+	ISNULL(CASE WHEN A_DATES.[6] = (SELECT MIN(cast(TransactionDate as date)) FROM {{ ref('settlementresults') }}) THEN -A.[6] END,0)
+	ISNULL(CASE WHEN A_DATES.[7] = (SELECT MIN(cast(TransactionDate as date)) FROM {{ ref('settlementresults') }}) THEN -A.[7] END,0)
+	ISNULL(CASE WHEN A_DATES.[8] = (SELECT MIN(cast(TransactionDate as date)) FROM {{ ref('settlementresults') }}) THEN -A.[8] END,0)
+	ISNULL(CASE WHEN A_DATES.[9] = (SELECT MIN(cast(TransactionDate as date)) FROM {{ ref('settlementresults') }}) THEN -A.[9] END,0)
+	ISNULL(CASE WHEN A_DATES.[10] = (SELECT MIN(cast(TransactionDate as date)) FROM {{ ref('settlementresults') }}) THEN -A.[10] END,0)

)

Select * from CreditCard_Rec_SLTRANS
