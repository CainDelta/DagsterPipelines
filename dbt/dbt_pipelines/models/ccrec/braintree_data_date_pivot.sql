


{{ config(materialized='view', alias='BRAINTREE_DATA_DATE_PIVOT') }}

SELECT	CLIENTID,[1],[2],[3],[4],[5],[6],[7],[8],[9],[10]
--INTO	#BRAINTREE_DATA_Date_PIVOT
FROM    (SELECT CLIENTID,CREATED_AT,[Sequence] = row_number() over (partition by  CLIENTID order by CREATED_AT) FROM {{ ref('braintree_data') }} ) AS SRC
PIVOT	(MAX(CREATED_AT) FOR [Sequence] IN ( [1], [2], [3], [4], [5],[6],[7],[8],[9],[10])) AS PVT
