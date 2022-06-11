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

warnings.filterwarnings("ignore", category=ExperimentalWarning)


##Set up DBT resources
my_dbt_resource = dbt_cli_resource.configured({"project_dir": "C:/Users/Arthur.Mubaiwa/Desktop/Brav/oApiData/Dagster/dbt/dbt_pipelines","models": ["tag:pre_summary_table"], "profiles_dir": "C:/Users/Arthur.Mubaiwa/.dbt/"})
dbt_assets = load_assets_from_dbt_project(project_dir= "C:/Users/Arthur.Mubaiwa/Desktop/Bravo/ApiData/Dagster/dbt/dbt_pipelines",select=['create_events_table','emails_sent','group_emails_sent','grouped_events'],profiles_dir="C:/Users/Arthur.Mubaiwa/.dbt/")
#dbt_assets_two = load_assets_from_dbt_project(project_dir= "C:/Users/Arthur.Mubaiwa/Desktop/Bravo/dbt/dbt_pipelines",select=['braintree_data'],profiles_dir="C:/Users/Arthur.Mubaiwa/.dbt/")
#@op(resource_defs={"dbt": my_dbt_resource})
# @op(required_resource_keys={"dbt"})
# def run_dbt_section() -> DbtCliOutput:
#     dbt = dbt_run_op()
#     return dtb
    #return context.resources.dbt.run()

##DOWNLOAD FUNCTION
def downloadRawData(grouped_events,campaign_label):
    """Function downloads raw data from the Bennetts_MI database """

    sqlcon = create_engine(dbstring)
    df = pd.read_sql(f"select * from Sitecore.Grouped_Events where [Campaign Display Label] = '{campaign_label}' ",sqlcon)
    df['Tracking Datetime'] = pd.to_datetime(df['Tracking Datetime'])
    df.sort_values(by=['BBIS Client Key','Campaign ID','Message ID','Campaign Display Label','Tracking Datetime'],ascending=True,inplace=True)

    return df


##PROCESS EMAIL
def getEmailId(downloadRawData):

    """Function uses a loop to create an Email sent ID by copying the EventID for row with 'Email Sent' in description """

    df_list = downloadRawData.copy().values.tolist()
    new_df_list = []
    failures = []

    for row in df_list:
        new_row = row.copy()
        try:
            if(row[5] == 'Email Sent'):
                new_row.append(new_row[0])
            else:
                #Get last row of data , assumes last row is either Email Sent or another open related to the same email which has already been processed
                prev_row_id = new_df_list[len(new_df_list)-1][8]
                new_row.append(prev_row_id)
            new_df_list.append(new_row)
        except Exception as e:
            failures.append(row)




    ##Get Column names and append new id
    colnames = downloadRawData.columns.tolist()
    colnames += ['EmailID']
    processed_df = pd.DataFrame(new_df_list,columns=colnames)

    return processed_df

###download campaign data by label
@asset(compute_kind='python')
def downloadRawData_R5(grouped_events):
    """Function downloads R5 RAW DATA from the Bennetts_MI database """

    df = downloadRawData(grouped_events,'R-5 Email')

    return df

###download campaign data by label
@asset(compute_kind='python')
def downloadRawData_R14(grouped_events):
    """Function downloads R14 RAW DATA from the Bennetts_MI database """

    df = downloadRawData(grouped_events,'R-14 Email')

    return df

###download campaign data by label
@asset(compute_kind='python')
def downloadRawData_R40(grouped_events):
    """Function downloads R40 RAW DATA from the Bennetts_MI database """

    df = downloadRawData(grouped_events,'R-40 Email')

    return df

###download campaign data by label
@asset(compute_kind='python')
def downloadRawData_RP5(grouped_events):
    """Function downloads RP5 RAW DATA from the Bennetts_MI database """

    df = downloadRawData(grouped_events,'RP-5 Email')

    return df



###MAIN PYTHON SECTION PROCESSING
@asset(compute_kind='python')
def getEmailId_R5(downloadRawData_R5):

    """Function uses a loop to create an Email sent ID by copying the EventID for row with 'Email Sent' in description  -- R5"""
    processed_df = getEmailId(downloadRawData_R5)

    return processed_df

@asset(compute_kind='python')
def getEmailId_R14(downloadRawData_R14):

    """Function uses a loop to create an Email sent ID by copying the EventID for row with 'Email Sent' in description  -- R14"""
    processed_df = getEmailId(downloadRawData_R14)

    return processed_df

@asset(compute_kind='python')
def getEmailId_R40(downloadRawData_R40):

    """Function uses a loop to create an Email sent ID by copying the EventID for row with 'Email Sent' in description  -- R40"""
    processed_df = getEmailId(downloadRawData_R40)

    return processed_df

@asset(compute_kind='python')
def getEmailId_RP5(downloadRawData_RP5):

    """Function uses a loop to create an Email sent ID by copying the EventID for row with 'Email Sent' in description  -- RP5"""
    processed_df = getEmailId(downloadRawData_RP5)

    return processed_df

##################### UPLOADS ################


@asset(compute_kind='python')
def uploadDataR5(getEmailId_R5):
    """Upload processed data to the MI database -- Misc.EmailEvents R5 """

    sqlcon = create_engine(dbstring,fast_executemany=True)
    getEmailId_R5.to_sql('EmailEvents', sqlcon,method='multi', index=False, if_exists="replace", schema="Sitecore",chunksize=200)
    get_dagster_logger().info(f"Uploaded {len(getEmailId_R5)} rows to EmailEvents")
    return 'uploaded'

###takes first upload as an input to form a chain of uploads appending to EmailEventsTable
@asset(compute_kind='python')
def uploadDataRP5(getEmailId_RP5,uploadDataR5):
    """Upload processed data to the MI database -- Misc.EmailEvents R5 """

    sqlcon = create_engine(dbstring,fast_executemany=True)
    getEmailId_RP5.to_sql('EmailEvents', sqlcon,method='multi', index=False, if_exists="append", schema="Sitecore",chunksize=200)
    get_dagster_logger().info(f"Uploaded {len(getEmailId_RP5)} rows to EmailEvents")
    return 'uploaded'

@asset(compute_kind='python')
def uploadDataRP14(getEmailId_R14,uploadDataRP5):
    """Upload processed data to the MI database -- Misc.EmailEvents R5 """

    sqlcon = create_engine(dbstring,fast_executemany=True)
    getEmailId_R14.to_sql('EmailEvents', sqlcon,method='multi', index=False, if_exists="append", schema="Sitecore",chunksize=200)
    get_dagster_logger().info(f"Uploaded {len(getEmailId_R14)} rows to EmailEvents")
    return 'uploaded'

@asset(compute_kind='python')
def uploadDataRP40(getEmailId_R40,uploadDataRP14):
    """Upload processed data to the MI database -- Misc.EmailEvents R5 """

    sqlcon = create_engine(dbstring,fast_executemany=True)
    getEmailId_R40.to_sql('EmailEvents', sqlcon,method='multi', index=False, if_exists="append", schema="Sitecore",chunksize=200)
    get_dagster_logger().info(f"Uploaded {len(getEmailId_R40)} rows to EmailEvents")
    return 'uploaded'



##RUN DBT POST PROCESSING PIPELINES
@asset(compute_kind='dbt',required_resource_keys={"dbt"})
def run_dbt_section(context,uploadDataRP40) -> DbtCliOutput:
    #dbt = dbt_run_op()
    return context.resources.dbt.run(models=["pre_summary_table","summary_table"])

asset_group = AssetGroup(dbt_assets + [downloadRawData_R5,downloadRawData_R14,downloadRawData_RP5,
                downloadRawData_R40,getEmailId_R5,getEmailId_R14,getEmailId_R40,getEmailId_RP5,uploadDataR5,uploadDataRP5,uploadDataRP14,uploadDataRP40] + [run_dbt_section],
              resource_defs={"dbt":  dbt_cli_resource.configured({"project_dir": "C:/Users/Arthur.Mubaiwa/Desktop/Bravo/ApiData/Dagster/dbt/dbt_pipelines", "profiles_dir": "C:/Users/Arthur.Mubaiwa/.dbt/"}),
              },
              )


###job that strings together the ops
sitecore_analytics = asset_group.build_job(name="Sitecore_Marketing_Processing")
