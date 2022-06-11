{{ config(materialized='incremental',schema='MISC',alias='aquotes_channels') }}


select
       case
            when (   PRICEMATCH_PKEY is not null
               and   BLOCKEDCANDIDATE = 1) then 'A-Quote (Direct Matched) : Blocked'
            when PRICEMATCH_PKEY is not null then 'A-Quote (Direct Matched)'
            when BLOCKEDCANDIDATE = 1 then 'A-Quote : Blocked'
            else 'A-Quote' end as "Transaction Sub Type",
       ATCRDT as "Transaction Loaded Datetime",
       ATLCDT as "Transaction Changed Datetime",
       AGPPKEY as "BBIS PQuote Key",
       QMRKEY as "BBIS Risk Key",
       cast('0' as number(5)) as "BBIS Renewal Sequence",
       cast('0' as number(5)) as "BBIS Amendment Sequence",
       --((((cast(QMRKEY as varchar(20)) + '|') + cast(0 as varchar(3))) + '|') + cast(0 as varchar(3))) as "BBIS Risk Linker",
       cast(null as varchar(28)) as "BBIS Risk Linker Next",

       'Unknown' as "Policy Document Delivery Method",
       upper(QMOPID) as "Quote Operator ID",
       case
            when (   MMLEV1 = 224
               and   QMOPID = 'INET') then 'Aggregator'
            when QMOPID = 'INET' then 'Internet'
            when QMOPID <> 'INET' then 'Call Centre' end as "Quote Channel",
       'Aggregator' as "Quote Initial Channel",
       case
            when (   MMLEV1 = 224
               and   QMOPID = 'INET'
               and   PMAST_ATCRDT is null) then 'Aggregator'
            when (   QMOPID = 'INET'
               and   coalesce(PMAST_QMOP2, '') in ( '', 'INET' )) then 'Internet'
            when coalesce(PMAST_QMOP2, '')not in ( '', 'INET' ) then 'Call Centre' end as "Quote Last Touch Channel",
       upper(MCOP2) as "Sale Operator ID",
       case
            when MCOP2 = 'INET' then 'Internet'
            when MCOP2 <> 'INET' then 'Call Centre'
            when (   MMLEV1 = 224
               and   MCOP2 = 'INET') then 'Aggregator' end as "Sale Channel"


from {{source('bennetts_mi','aquotes') }}

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where ATCRDT > (select max( "Transaction Loaded Datetime") from {{ this }})

{% endif %}
