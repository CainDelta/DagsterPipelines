import pandas as pd
import os
from sqlalchemy import create_engine
from dagster import job, op, get_dagster_logger,InputDefinition,Nothing
from dagster_dbt import dbt_cli_resource, dbt_run_op
from dagster_dbt.cli.types import DbtCliOutput
from dagster import AssetGroup, asset,ExperimentalWarning
from dagster_dbt import dbt_cli_resource, load_assets_from_dbt_project
import warnings
from credentials import dbstring 


dbt_assets = load_assets_from_dbt_project(project_dir= "C:/Users/Arthur.Mubaiwa/Desktop/Bravo/ApiData/Dagster/dbt/dbt_pipelines",select=['botchedcorrection','botchedlapsed','genuinelapsed','lapsed_I_ledger','lapsedcorrections','lapsedcorrections_inc','lapsedpayments','ledger'],profiles_dir="C:/Users/Arthur.Mubaiwa/.dbt/")

@asset(compute_kind='python')
def getStats(lapsedpayments):
        sqlcon = create_engine(dbstring)
        df = pd.read_sql("select * from Testing.T_Lapsed_Payments",sqlcon)
        get_dagster_logger().info(f"Uploaded {len(df)} rows to Lapsed Payments")

        return 'Job Done'


asset_group = AssetGroup(dbt_assets + [getStats],
              resource_defs={"dbt":  dbt_cli_resource.configured({"project_dir": "C:/Users/Arthur.Mubaiwa/Desktop/Bravo/dbt/dbt_pipelines", "profiles_dir": "C:/Users/Arthur.Mubaiwa/.dbt/"})}
              )

lapsed_payments = asset_group.build_job(name="Lapsed_Payments_Process")
