from dagster import job
from dagster_dbt import dbt_cli_resource, dbt_run_op


##Set up DBT resources
my_dbt_resource = dbt_cli_resource.configured({"project_dir": "C:/Users/Arthur.Mubaiwa/Desktop/Bravo/dbt/bennetts_mi", "profiles_dir": "C:/Users/Arthur.Mubaiwa/.dbt/"})

##Run DBT job
@job(resource_defs={"dbt": my_dbt_resource})
def Sitecore_DBT():
    dbt_run_op()
