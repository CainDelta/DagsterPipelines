{{ config(materialized='incremental',schema='MISC',alias='master_aquotes') }}

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
       ATLCDT as "Transaction Changed Datetime",
       cast(null as varchar(4)) as "Transaction Override Operator ID",
       cast(null as varchar) as "Transaction Override Operator Name",
       cast(null as varchar(200)) as "Transaction Held Report Reason",
       cast(null as timestamp_ntz) as "Transaction Held Report Date Held",
       cast(null as timestamp_ntz) as "Transaction Held Report Date Released",
       cast(null as varchar(200)) as "Transaction Document Link",
       MCACDT as "Policy Acceptance Date",
       MCDATE as "Policy Inception Date",
       MCEXDATE as "Policy Expiry Date",
       CANC_MCDATE as "Policy Cancellation Effective Date",
       cast(null as timestamp_ntz) as "Policy Cancellation Loaded Datetime",
       cast(null as varchar) as "Policy Cancellation Type",
       cast(null as varchar(100)) as "Policy Cancellation Reason",
       AGPPKEY as "BBIS PQuote Key",
       QMRKEY as "BBIS Risk Key",
       cast('0' as number(5)) as "BBIS Renewal Sequence",
       cast('0' as number(5)) as "BBIS Amendment Sequence",
       --((((cast(QMRKEY as varchar(20)) + '|') + cast(0 as varchar(3))) + '|') + cast(0 as varchar(3))) as "BBIS Risk Linker",
       cast(null as varchar(28)) as "BBIS Risk Linker Next",
       TRAN_SEQ as "Transaction Sequence",
       TRAN_SEQ_REV as "Transaction Sequence Rev",
       QMCKEY as "BBIS AClient Key",
       PMAST_QMCKEY as "BBIS Client Key",
       "ADOBE CLIENT KEY" as "Adobe Campaign Client Key",
       "Visitor Cloud ID" as "Adobe Analytics Visitor ID",
       cast(null as varchar(300)) as "SessionCam URL",
       PRICEMATCH_PKEY as "BBIS Price Matched Quote Key",
       cast(null as varchar(30)) as "BBIS Invite Match Linker",
       PREVIOUS_ATCRDT as "Previous AQuote Loaded Datetime",
       cast(null as number(10)) as "BBIS Predecessor Risk Key",
       cast(null as number(10)) as "BBIS Predecessor Renewal Sequence",
       cast(null as varchar(30)) as "BBIS Predecessor Type",
       cast(null as number(10)) as "BBIS Predecessor Days Since Cancellation",
       cast(null as number(10)) as "BBIS Successor Risk Key",
       cast(null as varchar(20)) as "PQuote IP Address",
       'Unknown' as "Policy Passive Renewal",
       'Unknown' as "Transaction Passive Renewal Detail",
       cast(null as varchar(5)) as "Policy Currently Passive Blocked",
       cast(null as timestamp_ntz) as "Policy Passive Block Date",
       cast(null as varchar(50)) as "LastUpdatedByPassiveBlock",
       cast(null as varchar(10)) as "LastUpdatedUser",
       cast(null as varchar(50)) as "LastestPassiveBlockSource",
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
               and   MCOP2 = 'INET') then 'Aggregator' end as "Sale Channel",
       upper(QMOP2) as "Transaction Operator ID",
       cast(null as varchar(3)) as "Transaction Failed Payment",
       cast(null as varchar(3)) as "Transaction Renewal Accessed"

from {{source('bennetts_mi','aquotes') }}

{% if is_incremental() %}
  -- this filter will only be applied on an incremental run
  where ATCRDT > (select max( "Transaction Loaded Datetime") from {{ this }})

{% endif %}
