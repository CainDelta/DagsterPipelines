
	{{ config(materialized='ephemeral', alias='COMBINEDTransactionLog') }}

	SELECT * FROM Bennetts_Snapshot.Payment.TransactionLog WHERE TransactionDate >= DATEADD(DD,-365,GETDATE())
	UNION ALL
	SELECT * FROM Bennetts_MI.Policies.T_CreditCard_TransactionLog_Today WHERE TransactionLogPK NOT IN (SELECT TransactionLogPK FROM Bennetts_Snapshot.Payment.TransactionLog)
