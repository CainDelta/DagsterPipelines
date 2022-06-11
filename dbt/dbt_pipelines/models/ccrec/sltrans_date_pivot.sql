

{{ config(materialized='view', alias='SLTRANS_DATE_PIVOT') }}

---- Pivot by Client, to get all ledger dates:
SELECT	CRPKEY,[1],[2],[3],[4],[5],[6],[7],[8],[9],[10],[11],[12],[13],[14],[15]
--INTO	#SLTRANS_DATE_PIVOT
FROM    (SELECT CRPKEY,STDATE,[Sequence] = row_number() over (partition by  CRPKEY order by STPKEY) FROM {{ ref('sltrans') }} S ) AS SRC
PIVOT	(MAX(STDATE) FOR [Sequence] IN ( [1], [2], [3], [4], [5],[6],[7],[8],[9],[10],[11],[12],[13],[14],[15])) AS PVT
