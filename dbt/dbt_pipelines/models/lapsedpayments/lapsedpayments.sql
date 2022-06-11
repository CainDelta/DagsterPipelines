{{ config(materialized='table', alias='T_Lapsed_Payments') }}

select * from {{ ref('botchedlapsed') }}
union all
select * from {{ ref('botchedcorrection') }}
union all
select * from {{ ref('genuinelapsed') }}
union all
select * from {{ ref('lapsed_I_ledger') }}
union all
select * from {{ ref('lapsedcorrections_inc') }}
union all
select * from {{ ref('lapsedcorrections') }}
union all
select b.*, hashkey = null from Misc.V_Underpayments b
