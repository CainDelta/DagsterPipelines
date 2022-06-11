import pandas as pd
import os
from sqlalchemy import create_engine
from dagster import job, op, get_dagster_logger,InputDefinition, Nothing, OutputDefinition
from dagster_dbt import dbt_cli_resource, dbt_run_op
from dagster_dbt.cli.types import DbtCliOutput
from credentials import dbstring


##Set up DBT resources
my_dbt_resource = dbt_cli_resource.configured({"project_dir": "C:/Users/Arthur.Mubaiwa/Desktop/Bravo/dbt/dbt_pipelines", "profiles_dir": "C:/Users/Arthur.Mubaiwa/.dbt/"})



@op(required_resource_keys={"dbt"}, input_defs=[InputDefinition("after", Nothing)])
def run_dbt_section(context) -> DbtCliOutput:
    #dbt = dbt_run_op()
    return context.resources.dbt.run()

@op(input_defs=[InputDefinition("after", Nothing)])
def downloadRawData():
    """Function downloads raw data from the Bennetts_MI database """

    sqlcon = create_engine(dbstring)
    df = pd.read_sql('select * from Testing.Grouped_Events',sqlcon)
    df['Tracking Datetime'] = pd.to_datetime(df['Tracking Datetime'])
    df.sort_values(by=['BBIS Client Key','Campaign ID','Message ID','Campaign Display Label','Tracking Datetime'],ascending=True,inplace=True)

    return df

@op
def getEmailId(df):

    """Function uses a loop to create an Email sent ID by copying the EventID for row with 'Email Sent' in description """

    df_list = df.copy().values.tolist()
    new_df_list = []
    for row in df_list:
        new_row = row.copy()
        if(row[1] == 'Email Sent'):
            new_row.append(new_row[0])
        else:
            #Get last row of data , assumes last row is either Email Sent or another open related to the same email which has already been processed
            prev_row_id = new_df_list[len(new_df_list)-1][8]
            new_row.append(prev_row_id)

        new_df_list.append(new_row)


    ##Get Column names and append new id
    colnames = df.columns.tolist()
    colnames += ['EmailID']
    processed_df = pd.DataFrame(new_df_list,columns=colnames)

    return processed_df

@op
def uploadData(processed_df):
    """Upload processed data to the MI database -- Misc.EmailEvents """

    sqlcon = create_engine(dbstring,fast_executemany=True)
    processed_df.to_sql('EmailEvents', sqlcon,method='multi', index=False, if_exists="replace", schema="Misc",chunksize=200)
    get_dagster_logger().info(f"Uploaded {len(processed_df)} rows to EmailEvents")
    return 'uploaded'


###job that strings together the ops
@job(resource_defs={"dbt": my_dbt_resource})
def Sitecore_Marketing_Processing():
    #dbt = dbt_run_op()
    dbt = run_dbt_section()
    uploaded = uploadData(getEmailId(downloadRawData(after=dbt)))
    #runProcedure(uploaded)
