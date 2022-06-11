

  {{ config(materialized='view', alias='COMBINEDSETTLEMENTRESULTS') }}


  ---- Creates a hybrid of live and MI settlement results for the past year:
  WITH COMBINEDSETTLEMENTRESULTS AS (

	   SELECT * FROM Bennetts_Snapshot.Payment.SettlementResults WHERE TransactionDate >= DATEADD(DD,-365,GETDATE())
      AND  ISNUMERIC(Reference) <> 0 AND LEN(Reference) > 0
	    UNION ALL
	   SELECT * FROM Bennetts_MI.Policies.T_CreditCard_SettlementResults_Today WHERE [SettlementResultsPK] NOT IN (SELECT [SettlementResultsPK] FROM Bennetts_Snapshot.Payment.SettlementResults)
      AND  ISNUMERIC(Reference) <> 0 AND LEN(Reference) > 0
  )

  SELECT * FROM COMBINEDSETTLEMENTRESULTS
