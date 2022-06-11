import requests
import json
import io
import pandas as pd
from sqlalchemy import create_engine
import time
from dagster import job, op, get_dagster_logger,List,IOManager, io_manager,Out,AssetMaterialization,EventMetadataEntry,AssetKey,daily_partitioned_config,get_dagster_logger
import urllib3
from datetime import datetime
from credentials import dbstring,csat_auth

class DataframeTableIOManagerWithMetadata(IOManager):
    ##IO manager uploads to SQL database
    def handle_output(self, context, obj):
        sqlcon = create_engine(dbstring,fast_executemany=True)
        table_name = context.metadata["table"]
        schema = context.metadata["schema"]
        obj.to_sql(name=table_name, schema=schema,con=sqlcon,if_exists='replace',method='multi',index=False, chunksize=50)

         # attach these to the Handled Output event
        yield EventMetadataEntry.int(len(obj), label="number of rows")
        yield EventMetadataEntry.text(table_name, label="table name")

    def load_input(self, context):
        sqlcon = create_engine(dbstring,fast_executemany=True)
        table_name = context.upstream_output.metadata["table"]
        schema = context.upstream_output.metadata["schema"]
        return read_dataframe_from_table(name=table_name, schema=schema)


@io_manager
def df_table_io_manager(_):
    return DataframeTableIOManagerWithMetadata()

@op
def DownloadCSAT():
    """" Downlaods CSAT data from the flex website from 2021-01-01 to current date """

    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    uri = 'https://bennetts-flex-api.azurewebsites.net/api/Flex/feedback?startDate=2021-01-01&endDate=2023-03-30'

    headers = {
          'Accept': 'application/json',
          'Content-Type': 'application/json',
          'Authorization' : csat_auth
        }


    response = requests.request('GET', uri, headers=headers,verify=False)
    chat_json = json.loads(response.text)
    list_j = [json.dumps(x)for x in chat_json]
    jsonDF = pd.DataFrame(list_j, columns =['JSON'])
    expandedDF = pd.DataFrame.from_dict(chat_json)
    expandedDF['Loaded Datetime'] = datetime.now()
    return expandedDF

@op (out=Out(metadata={"schema": "Twilio", "table": "T_Webchat_CSAT"},io_manager_key="csat_mi"))
def uploadCSAT(expandedDF) -> pd.DataFrame:
    sqlcon = create_engine(dbstring)

    with sqlcon.begin() as conn:
        conn.execute('Truncate table Twilio.T_Webchat_CSAT_Staging')
        conn.execute('Truncate table Twilio.T_Webchat_CSAT')

    ##UPLOAD DATA to CSAT Table

    #jsonDF.to_sql(name='T_Webchat_CSAT_Staging',index=False,schema='Twilio',con=sqlcon,if_exists='append')
    #expandedDF.to_sql(name='T_Webchat_CSAT',index=False,schema='Twilio',con=sqlcon,if_exists='append')

    return expandedDF


@job(resource_defs={"csat_mi": df_table_io_manager})
def Twilio_Webchat_CSAT():
    ex = DownloadCSAT()
    uploadCSAT(ex)
