

{{ config(materialized='view', alias='BRAINTREE_DATA_POLICY_PIVOT') }}


WITH BRAINTREE_DATA_POLICY_PIVOT AS (

  SELECT	CLIENTID,[1],[2],[3],[4],[5],[6],[7],[8],[9],[10]
  --INTO	#BRAINTREE_DATA_Policy_PIVOT
  FROM    (SELECT CLIENTID,Policy_Quote,[Sequence] = row_number() over (partition by  CLIENTID order by CREATED_AT) FROM  {{ ref('braintree_data') }} ) AS SRC
  --FROM    (SELECT CLIENTID,RiskId,[Sequence] = row_number() over (partition by  CLIENTID order by CREATED_AT) FROM #BRAINTREE_DATA ) AS SRC
  --PIVOT	(MAX(RiskId) FOR [Sequence] IN ( [1], [2], [3], [4], [5],[6],[7],[8],[9],[10])) AS PVT
  PIVOT	(MAX(Policy_Quote) FOR [Sequence] IN ( [1], [2], [3], [4], [5],[6],[7],[8],[9],[10])) AS PVT

)

select * from BRAINTREE_DATA_POLICY_PIVOT
