from dagster import AssetGroup, asset,ExperimentalWarning
from dagster_dbt import dbt_cli_resource, load_assets_from_dbt_project
import warnings

warnings.filterwarnings("ignore", category=ExperimentalWarning)


@asset
def upstream():
    return [1, 2, 3]


@asset
def downstream_1(upstream):
    return upstream + [4]


@asset
def downstream_2(upstream):
    return len(upstream)


##Set up DBT resources
my_dbt_resource = dbt_cli_resource.configured({"project_dir": "C:/Users/Arthur.Mubaiwa/Desktop/Bravo/dbt/dbt_pipelines", "profiles_dir": "C:/Users/Arthur.Mubaiwa/.dbt/"})
dbt_assets = load_assets_from_dbt_project(project_dir= "C:/Users/Arthur.Mubaiwa/Desktop/Bravo/dbt/dbt_pipelines",profiles_dir="C:/Users/Arthur.Mubaiwa/.dbt/")




@asset(compute_kind='python')
def python_op(grouped_events):
    return 'Python step'


asset_group = AssetGroup(dbt_assets + [python_op],
              resource_defs={"dbt":  dbt_cli_resource.configured({"project_dir": "C:/Users/Arthur.Mubaiwa/Desktop/Bravo/dbt/dbt_pipelines", "profiles_dir": "C:/Users/Arthur.Mubaiwa/.dbt/"}),
              },
              )


all_assets = asset_group.build_job(name="my_asset_job")
#
# downstream_assets = asset_group.build_job(
#     name="my_asset_job", selection=["upstream", "downstream_1"]
# )
#
# upstream_and_downstream_1 = asset_group.build_job(name="my_asset_job", selection="*downstream_1")
# # end_marker
