

{{ config(materialized='view', alias='BRAINTREE_DATA') }}

WITH BRAINTREE_DATA AS (

SELECT
			CLIENTMAPPING.CLIENTID
,			AMOUNT = cast(BRAINTREE.AMOUNT as numeric(8,2))
,			BRAINTREE.CREATED_AT
,			RiskId = JSON_VALUE(Metadata,'$.RiskId')
,			RenewalSequence = JSON_VALUE(Metadata,'$.RenewalSequence')
,			QuoteId = JSON_VALUE(Metadata,'$.QuoteId')
,			Metadata
,           TransactionID = JSON_VALUE(Metadata,'$.BraintreeResponseData.TransactionId')
,           ROW_NUMBER() OVER(ORDER BY Braintree.Created_At ASC) AS RowID
,           Policy_Quote =  COALESCE(JSON_VALUE(Metadata,'$.PolicyNumber'),JSON_VALUE(Metadata,'$.QuoteId'))
--INTO		#BRAINTREE_DATA

FROM		bennetts_mi.misc.T_Transactions							TRANSACTIONS
LEFT JOIN	Bennetts_Snapshot.dbo.ExternalClientMapping			CLIENTMAPPING
ON			ExternalClientMappingId = CustomerKey

INNER JOIN	Bennetts_MI.[Misc].[T_BrainTree_Transactions]		 BRAINTREE
ON			'ins-' + cast(TRANSACTIONS.TRANSACTIONID as varchar(200))=cast(BRAINTREE.ORDER_ID as varchar(200))
WHERE		BRAINTREE.MERCHANT_ACCOUNT_ID = 'InsuranceGBP' -- change to LIKE
and         BRAINTREE.status = 'settled'
-- and         BRAINTREE.refund_id is null
-- and         BRAINTREE.refunded_transaction_id is null

)

select  * from BRAINTREE_DATA
