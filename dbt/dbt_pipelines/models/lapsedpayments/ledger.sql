{{ config(materialized='ephemeral', alias='ledger') }}

select IDRISK,IDRSEQ,IDASEQ,IDEDAT ,IDTEXT,IDPAMT,IDPREM,IDDEP,IDINSU,
HASHBYTES('SHA2_256', CONCAT(IDRISK,IDRSEQ,IDASEQ,IDTEXT,IDEDAT,IDPAMT)) as HashKey
from bennetts.dbo.I_ledger a
