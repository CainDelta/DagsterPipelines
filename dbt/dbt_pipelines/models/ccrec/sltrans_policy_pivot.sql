

---- Pivot by Client, to get all ledger policy numbers:
{{ config(materialized='view', alias='SLTRANS_POLICY_PIVOT') }}


SELECT	CRPKEY,[1],[2],[3],[4],[5],[6],[7],[8],[9],[10],[11],[12],[13],[14],[15]
--INTO	#SLTRANS_POLICY_PIVOT
FROM    (SELECT CRPKEY,STPOLN,[Sequence] = row_number() over (partition by  CRPKEY order by STPKEY) FROM {{ ref('sltrans') }}  ) AS SRC
PIVOT	(MAX(STPOLN) FOR [Sequence] IN ( [1], [2], [3], [4], [5],[6],[7],[8],[9],[10],[11],[12],[13],[14],[15])) AS PVT
