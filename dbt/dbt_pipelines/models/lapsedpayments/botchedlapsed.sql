
{{ config(materialized='ephemeral', alias='botchedpayments') }}

select [BBIS Risk Key],[BBIS Renewal Sequence],[BBIS Amendment Sequence], [Policy Number],[Transaction Loaded Datetime],
[Transaction Effective Date],[Transaction Expiry Date], IDEDAT as [Date Effective],IDTEXT,IDPAMT,IDPREM as Premium,
[Payment Total Amount],[Transaction Type],[Transaction Sub Type],Category = 'Overpayment' ,a.IDINSU,[label] = 'Overpayment',a.HashKey

from {{ ref('ledger') }} a
left join Policies.T_Master_Policies_Full b
on a.IDRISK = b.[BBIS Risk Key]
and a.IDASEQ = b.[BBIS Amendment Sequence]
and a.IDRSEQ = b.[BBIS Renewal Sequence]
and [IDEDAT] = [Transaction Effective Date]
where [Transaction Effective Date] is not null
and [Transaction Type] in ('Renewal Invite (Lapsed)','Renewal Invited (Botched)')
