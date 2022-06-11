



{{ config(materialized='view', alias='SettlementResults_Policy_PIVOT') }}
---- Pivot by Client, to get all settlement policy numbers:
SELECT	[BBIS Client Key],[1],[2],[3],[4],[5],[6],[7],[8],[9],[10]
FROM    (SELECT [BBIS Client Key],[Policy Number],[Sequence] = row_number() over (partition by  [BBIS Client Key] order by [SettlementResultsPK]) FROM {{ ref('settlementresults') }} ) AS SRC
PIVOT	(MAX([Policy Number]) FOR [Sequence] IN ( [1], [2], [3], [4], [5],[6],[7],[8],[9],[10])) AS PVT
