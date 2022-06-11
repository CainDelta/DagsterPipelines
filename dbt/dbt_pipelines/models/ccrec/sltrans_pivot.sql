---- PIVOTS (Ledger Information):

---- Pivot by Client, to get all ledger amounts:
{{ config(materialized='view', alias='SLTRANS_PIVOT') }}

SELECT	CRPKEY,[1],[2],[3],[4],[5],[6],[7],[8],[9],[10],[11],[12],[13],[14],[15]
,		[STVAL_Full] = isnull([1],0) + isnull([2],0) + isnull([3],0) + isnull([4],0) +  isnull([5],0) + isnull([6],0)+isnull([7],0) + isnull([8],0) + isnull([9],0) + isnull([10],0) + isnull([11],0)+isnull([12],0) + isnull([13],0) + isnull([14],0) + isnull([15],0)
--INTO	#SLTRANS_PIVOT
FROM    (SELECT CRPKEY,STVAL_WITHSIGN,[Sequence] = row_number() over (partition by  CRPKEY order by STPKEY) FROM {{ ref('sltrans') }} ) AS SRC
PIVOT	(SUM(STVAL_WITHSIGN) FOR [Sequence] IN ( [1], [2], [3], [4], [5],[6],[7],[8],[9],[10],[11],[12],[13],[14],[15])) AS PVT
