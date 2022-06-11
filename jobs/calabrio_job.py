import requests
import json
import io
import pandas as pd
from sqlalchemy import create_engine
import time
from dagster import job, op, get_dagster_logger
from credentials import calabrio_user, calabrio_pass,dbstring

#id = 'RP1_1572_0_uo7edue2df27berffv0qb'

def getReport(id):

    url = 'https://uk.calabriocloud.com/api/rest/authorize'

    payload = json.dumps({'userId':calabrio_user,'password':calabrio_pass,'language':'en'})

    ##Authentication
    response = requests.request('POST', url, data=payload,verify=False)
    token = json.loads(response.text)['sessionId']


    headers = {
          'Accept': 'application/json',
          'Content-Type': 'application/jsonV',
          'cookie' : "hazelcast.sessionId="+token
        }

    ##Set report requirments and specifications in the body
    body = json.dumps({
       "schemaName":"com.stytch.rest.api.v4.data.report.action.ExportActionData",
       "action":"EXPORT_CSV",
       "state":{
          "schemaName":"com.stytch.rest.api.v4.data.report.ReportSpecData",
          "qname":id
       },
       "paramValues":{
          "schemaName":"com.stytch.rest.api.v4.data.parameters.EncodedParameterValuesData",
          "encodedParamValues":"[]",
          "qnameContent":id
       },
       "pageSpecification":[
          {
             "offset":0,
             "limit":20000
          },
          {
             "offset":0,
             "limit":20000
          }]
          })


    report_url = 'https://uk.calabriocloud.com/api/rest/dataexplorer/report/'+ id+ '/export'
    report_response = requests.request('POST', report_url, headers=headers,data=body,verify=False)
    evaluations = pd.read_csv(io.StringIO(report_response.text))
    evaluations
    #evaluations['Date Evaluated']= pd.to_datetime(evaluations['Date Evaluated'])

    ##For big reports the api might return a request ID and not a report. In this case, a new request based on that requestID is called
    if len(evaluations) > 0 :
        return evaluations
    else:
        reqID = json.loads(evaluations.columns[0]+'}')['requestId']
        report_url = 'https://uk.calabriocloud.com/api/rest/dataexplorer/report/export/request/'+reqID
        report_response = requests.request('GET', report_url, headers=headers,data=body,verify=False)
        evaluations = pd.read_csv(io.StringIO(report_response.text))
        ##Second IF statement if report is still in processing or queued with a  60second wait time to allow for processing then re-run
        if len(evaluations) == 0:
            response = json.loads(evaluations.columns[1].replace('status','{"status"'))['status']
            if response == 'PROCESSING' or response == 'QUEUED':
                time.sleep(120)
                print('Processing on Server side waiting for 60 seconds')
                report_url = 'https://uk.calabriocloud.com/api/rest/dataexplorer/report/export/request/'+reqID
                report_response = requests.request('GET', report_url, headers=headers,data=body,verify=False)
                report = pd.read_csv(io.StringIO(report_response.text))
                print('length of report is ', len(report))
            return report
        else:
            return evaluations



##Evaluations Report
@op
def Download_Evaluations():

    evaluations = getReport("RP1_1487_0_83tsr0hm2hr4hb70jc8dm")
    get_dagster_logger().info(f"Downloaded {len(evaluations)} rows")
    evaluations['ID'] = evaluations['Date Evaluated'] + '-' + evaluations['Agent'].astype(str) + '-' + evaluations['Evaluator']
    evaluations['Date Evaluated'] = pd.to_datetime(evaluations['Date Evaluated']) ##convert to datetime
    return evaluations

##Download Telephony data
@op
def Download_Telephony():
    telephony = getReport("RP1_1572_0_uo7edue2df27berffv0qb")
    get_dagster_logger().info(f"Downloaded {len(telephony)} rows")
    telephony['ID'] = telephony['Date Evaluated'] + '-' + telephony['Question'] + '-' + telephony['Evaluator'] ##Create ID
    telephony['Date Evaluated'] = pd.to_datetime(telephony['Date Evaluated'])
    return telephony

##Delete last 30 days in Bennetts_MI to account for any changes in Calabrio in the last month
@op
def Delete_Last30_Days_Telephony(Telephony):

    sqlcon = create_engine(dbstring)
    with sqlcon.begin() as conn:
        conn.execute("""delete from Calabrio.T_Telephony_Staging
              	 where cast([Date Evaluated] as date) >= cast(getdate()-10 as date)""")
    return Telephony


@op
def Delete_Last30_Days_Evaluations(Evaluations):
    sqlcon = create_engine(dbstring)
    with sqlcon.begin() as conn:
        conn.execute(""" delete from Calabrio.All_Evaluations_Staging
        	 where cast([Date Evaluated] as date) >= cast(getdate()-31 as date)""")
    return Evaluations



@op
def Upload_Telephony(Telephony):
    sqlcon = create_engine(dbstring)
    Telephony.to_sql(name='T_Telephony_Staging',index=False,method='multi',schema='Calabrio',con=sqlcon,if_exists='append', chunksize=50)
    get_dagster_logger().info(f"Uploaded {len(Telephony)} rows to T_Telephony_Staging")
    return len(Telephony)

@op
def Upload_Evaluations(Evaluations):
    sqlcon = create_engine(dbstring)
    Evaluations.to_sql(name='All_Evaluations_Staging',index=False,method='multi',schema='Calabrio',con=sqlcon,if_exists='append', chunksize=50)
    get_dagster_logger().info(f"Uploaded {len(Evaluations)} rows to All_Evaluations_Staging")
    return len(Evaluations)


@op
def Execute_StoredProcedures(Evaluations,Telephony):
    sqlcon = create_engine(dbstring)
    with sqlcon.begin() as conn:
        conn.execute("exec Calabrio.SP_Calabrio_LoadTelephony")
        conn.execute("exec Calabrio.SP_Calabrio_LoadEvaluations")


@job
def Calabrio_Job():
    e = Download_Evaluations()
    t = Download_Telephony()
    Execute_StoredProcedures(Upload_Telephony(Delete_Last30_Days_Telephony(t)),Upload_Evaluations(Delete_Last30_Days_Evaluations(e)))
