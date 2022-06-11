

{{ config(materialized='view', alias='SettlementResults_PIVOT') }}

SELECT	[BBIS Client Key],[1],[2],[3],[4],[5],[6],[7],[8],[9],[10]
,		[Settlement_Amount_Full] = isnull([1],0) + isnull([2],0) + isnull([3],0) + isnull([4],0) + isnull([5],0) + isnull([6],0) + isnull([7],0) + isnull([8],0) + isnull([9],0) + isnull([10],0)
FROM    (SELECT [BBIS Client Key],[Amount_WithSign],[Sequence] = row_number() over (partition by  [BBIS Client Key] order by [SettlementResultsPK]) FROM {{ ref('settlementresults') }}	) AS SRC
PIVOT	(SUM([Amount_WithSign]) FOR [Sequence] IN ( [1], [2], [3], [4], [5],[6],[7],[8],[9],[10])) AS PVT
