---- Tidies up, attaches relevant ledger codes, and client information
----------------------------
----   SALES LEDGER     ----
----------------------------


{{ config(materialized='view', alias='SLTRANS') }}
SELECT 		STPKEY
,			STPOLN
,			STRSEQ
,			STASEQ
,			STDATE=CAST(STDATE AS DATE)
,			STVAL_WITHSIGN	=	CASE		WHEN STITYP IN (SELECT LedgerCode FROM Bennetts_Snapshot.Payment.ReconcileLedgerCode WHERE RefundEntry = 1 OR ContraEntry = 1)
											THEN	-STVAL
											ELSE	STVAL
											END
,			CRPKEY
--INTO #SLTRANS
FROM {{ ref('sltrans_sample') }}	 AS strans
LEFT JOIN Bennetts_Snapshot.dbo.SLCODES AS scodes ON scodes.SFTRAN = strans.STITYP
LEFT JOIN Bennetts_Snapshot.dbo.CLIENT AS client ON client.CRPKEY = strans.STCKEY
WHERE cast(strans.STDATE as date) BETWEEN dateadd(dd,-35,getdate()) AND getdate()		---- Past 35 days of information
AND		strans.STITYP IN (SELECT LedgerCode FROM Bennetts_Snapshot.Payment.ReconcileLedgerCode)	---- Only pecific reconciliation codes
