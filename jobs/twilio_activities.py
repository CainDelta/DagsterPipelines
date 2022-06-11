import requests
import json
import io
import pandas as pd
from sqlalchemy import create_engine
import time
from dagster import job, op, get_dagster_logger,List,IOManager, io_manager,Out,AssetMaterialization,EventMetadataEntry,AssetKey,daily_partitioned_config,get_dagster_logger
import urllib3
from datetime import datetime
from credentials import twilio_pass,twilio_user,dbstring

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

###Get Token Function , returns TT
def GetToken():

    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    '''
    Function does a step call of the API to generate the SST and Temporary Token
    The Temporary Token is used for subsequent API calls
    '''

    ##STAGE !
    url = 'https://analytics.ytica.com/gdc/account/login'
    credentials = {'postUserLogin': {'login': twilio_user,
            'password': twilio_pass,
            'remember': 0,
            'verify_level': 2}
    }

    payload = json.dumps(credentials)

    headers = {
      'Accept': 'application/json',
      'Content-Type': 'application/json'
    }

    response = requests.request('POST', url, headers=headers, data=payload,verify=False)

    responseJson = json.loads(response.text)
    token = responseJson['userLogin']['token']

    ##STAGE 2


    headers = {
      'Accept': 'application/json',
      'Content-Type': 'application/json',
      'X-GDC-AuthSST': token
    }


    url = 'https://analytics.ytica.com/gdc/account/token'
    response = requests.request('GET', url, headers=headers,verify=False)
    TT = json.loads(response.text)['userToken']['token']

    return TT

def DownloadReport(Workspace,Object):

    Workspace  = 'c4ihsy0eehx96mzzaqilibu6kteiwiuq' # LIVE

    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    headers = {
      'Accept': 'application/json',
      'Content-Type': 'application/json'
    }

    body = {
      "report_req": {
        "report": "/gdc/md/"+ str(Workspace) +"/obj/"+ str(Object)
      }
    }
    payload = json.dumps(body)

    ##Set authentication cookie for future requests, relies on GetToken function
    cookie = {
        'domain':'analytics.ytica.com',
        'name':'GDCAuthTT',
        'path':'/',
        'value':GetToken()
    }

    s = requests.Session()
    s.cookies.set(**cookie)
    WorkspaceLIVE  = 'c4ihsy0eehx96mzzaqilibu6kteiwiuq' # LIVE
    URI = 'https://analytics.ytica.com/gdc/app/projects/' + Workspace + '/execute/raw'


    ##Download Report and convert to CSV
    response = s.request('POST', url=URI, headers=headers, data=payload,verify=False)
    report_uri = json.loads(response.text)['uri']
    final_url = 'https://analytics.ytica.com/' + report_uri
    report_response = s.request('GET', url=final_url, headers=headers, data=payload,verify=False)

    ##Conversion to csv
    decoded_content = report_response.content.decode('utf-8')
    report = pd.read_csv(io.StringIO(decoded_content))

    return report



@op
def GetActivites():
    """Runs function to get Activities report 12357 """
    ###Set Workspace and object names
    WorkspaceLIVE  = 'c4ihsy0eehx96mzzaqilibu6kteiwiuq' # LIVE
    object = '12357'
    for i in range(2):
        Activities = DownloadReport(WorkspaceLIVE,object)

    ##bennetts conversion to match existing procedures]
    Activities.fillna("",inplace=True) ##replace empty strings
    Activities['JSON'] = Activities.apply(lambda x: x.to_json(), axis=1)
    Activities['Loaded Datetime'] = datetime.now()
    Activities['JSON_identifier'] = Activities['Agent'] + '|' + Activities['Date'] + '|' + Activities['Time'] + '|' + Activities['Activity']
    Activities = Activities[['JSON','Loaded Datetime','JSON_identifier']]

    return Activities

@op(out=Out(metadata={"schema": "Twilio", "table": "T_Twilio_Activity_Staging"},io_manager_key="activities_mi"))
def loadActivities(Activities):
    """Load Activities into Staging table """
    get_dagster_logger().info(f"Downloaded {len(Activities)} activities")
    return Activities


@job(resource_defs={"activities_mi": df_table_io_manager})
def Twilio_Activities_Job():
    Activities = GetActivites()
    loadActivities(Activities)
