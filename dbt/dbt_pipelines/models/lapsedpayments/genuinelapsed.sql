
{{ config(materialized='ephemeral', alias='GenuineLapsed') }}

select [BBIS Risk Key],[BBIS Renewal Sequence],[BBIS Amendment Sequence], [Policy Number],[Transaction Loaded Datetime],
[Transaction Effective Date],[Transaction Expiry Date], IDEDAT as [Date Effective],IDTEXT,IDPAMT,IDPREM as Premium,
[Payment Total Amount],[Transaction Type],[Transaction Sub Type],Category = 'Overpayment',a.IDINSU,[label] = 'Overpayment',a.HashKey

from Policies.T_Master_Policies_Full b
left join {{ ref('ledger') }} a
on a.IDRISK = b.[BBIS Risk Key]
and a.IDASEQ = b.[BBIS Amendment Sequence]
and a.IDRSEQ = b.[BBIS Renewal Sequence]
where [Transaction Sub Type] in ('Renewal Invite (Lapsed)')
and a.IDASEQ is not null
