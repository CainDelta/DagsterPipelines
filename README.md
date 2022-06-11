## MIDE
Data Engineering Pipelines  built primarily in Python all controlled by Dagster. Slowly developing the platform to switch to a more 'Modern Data Stack' approach, at the moment 
intergrated Dagster & dbt. Next Steps are Airbyte + Looker to move away from  writing jobs in Python.

### Tech Used 
- Dagster , heart of the set up. All orchestration,schedules, asset materialization controlled by Dagster 
- Python , most of the jobs are Python jobs calling various APIs and updating the SQL Server / Snowflake databases 
- dbt for SQL modelling 
