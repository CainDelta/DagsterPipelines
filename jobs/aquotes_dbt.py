import requests
import json
import io
import pandas as pd
from sqlalchemy import create_engine
import time
from dagster_dbt import dbt_cli_resource, dbt_run_op
from dagster import job
import urllib3
from datetime import datetime,timedelta



my_dbt_resource = dbt_cli_resource.configured({"project_dir": "C:/Users/Arthur.Mubaiwa/Desktop/Bravo/ApiData/Dagster/dbt/dbt_pipelines","models": ["aquotes_channels"], 
                                                "profiles_dir": "C:/Users/Arthur.Mubaiwa/.dbt/"})


@job(resource_defs={"dbt": my_dbt_resource})
def aquotes_dbt_models():
    dbt_run_op()
