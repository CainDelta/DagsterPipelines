

{{ config(materialized='view', alias='BRAINTREE_DATA_PIVOT') }}

SELECT	CLIENTID,[1],[2],[3],[4],[5],[6],[7],[8],[9],[10]
,		[Braintree_Amount_Full] = isnull([1],0) + isnull([2],0) + isnull([3],0) + isnull([4],0) + isnull([5],0) + isnull([6],0) + isnull([7],0) + isnull([8],0) + isnull([9],0) + isnull([10],0)
--INTO	#BRAINTREE_DATA_PIVOT
FROM    (SELECT CLIENTID,AMOUNT,[Sequence] = row_number() over (partition by  CLIENTID order by CREATED_AT) FROM  {{ ref('braintree_data') }} ) AS SRC
PIVOT	(SUM(AMOUNT) FOR [Sequence] IN ( [1], [2], [3], [4], [5],[6],[7],[8],[9],[10])) AS PVT
