{{ config(materialized='incremental',schema='MISC',alias='aquotes_volumes') }}

select

'A-Quote' as "Transaction Type",
       case
            when (   PRICEMATCH_PKEY is not null
               and   BLOCKEDCANDIDATE = 1) then 'A-Quote (Direct Matched) : Blocked'
            when PRICEMATCH_PKEY is not null then 'A-Quote (Direct Matched)'
            when BLOCKEDCANDIDATE = 1 then 'A-Quote : Blocked'
            else 'A-Quote' end as "Transaction Sub Type",
       QMQEDT as "Quote Effective Date",
       ATCRDT as "Quote Loaded Datetime",
       ATLCDT as "Quote Changed Datetime",
       cast(null as number(10)) as "Quote PTX Personal Score",
       cast(null as number(10)) as "Quote PTX Address Score",
       PMAST_ATCRDT as "A-Quote ClickThru Datetime",
       cast(PMAST_ATCRDT as date) as "Transaction Acceptance Date",
       QMDATE as "Transaction Effective Date",
       dateadd(day, 28, QMQEDT) as "Transaction Expiry Date",
       cast(dateadd(day, 28, QMQEDT) as timestamp_ntz) as "Transaction Expiry Loaded Datetime",
       ATCRDT as "Transaction Loaded Datetime",
       ATLCDT as "Transaction Changed Datetime"


from {{source('bennetts_mi','aquotes') }}

{% if is_incremental() %}
  -- this filter will only be applied on an incremental run
  where ATCRDT > (select max( "Transaction Loaded Datetime") from {{ this }})

{% endif %}
