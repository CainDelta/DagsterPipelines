import requests
import json
import csv
import pandas as pd
import io
import pyodbc
from sqlalchemy import create_engine
import numpy as np
from dagster import job, op, get_dagster_logger
from credentials import jira_user,jira_password,dbstring



###Function authenticates and downlaods data
@op
def GetJiraData ():

    ##### AUTHENTICATION ##############
    url = "http://servicedesk/rest/auth/1/session"
    payload= json.dumps({'username':jira_user,'password':jira_password})
    headers = {
      'Content-Type': 'application/json'
    }

    response = requests.request("POST", url, headers=headers, data=payload)
    responseJson = json.loads(response.text)
    token = responseJson['session']['value']

    cookie = {
        'domain':'servicedesk',
        'name':'JSESSIONID',
        'path':'/',
        'value':token
    }

    responseJson
    "JSESSIONID="+token

    headers = {
      'Accept': 'application/json',
      'Content-Type': 'application/json',
      'Cookie': "JSESSIONID="+token
    }

    s = requests.Session()
    s.cookies.set(**cookie)


    ###DOWNLOAD DATA -----############
    url = 'http://servicedesk/rest/api/2/search?jql="Customer Request Type" = "Application Release" OR "Type" = "Service Request"  OR "Type" = "Service Request with Approvals"  ORDER BY updated desc&maxResults=1000&startAt=0'

    jira = s.request("GET", url, headers=headers)
    jira_requests = json.loads(jira.text)


    ##Convert to list of tuples with key and fields
    issues = [(jira_requests['issues'][x]['key'],json.dumps(jira_requests['issues'][x]['fields'])) for x in range(len(jira_requests['issues']))]
    issues_Df = pd.DataFrame(issues, columns =['key', 'JSON_Data']) ##Convert to dataframe for upload

    return issues_Df

###get jira
@op
def downloadJira():
    issues = GetJiraData()
    get_dagster_logger().info(f"Found {len(issues)} issues logged")
    return issues


##load data
@op
def uploadIssues(issues):
    sqlcon = create_engine(dbstring)
    issues.to_sql(name='T_JIRA_Staging',schema='Misc',con=sqlcon,if_exists='replace', index=False,method='multi', chunksize=50)
    get_dagster_logger().info(f"Uploaded {len(issues)} issues to T_JIRA_Staging")
    return 'uploaded'

## run procedure
@op
def runProcedure(uploaded):
    sqlcon = create_engine(dbstring)
    with sqlcon.begin() as conn:
        conn.execute('EXEC Bennetts_MI.Misc.SP_JIRA')

###job that strings together the ops
@job
def executeJIRA():
    uploaded = uploadIssues(downloadJira())
    runProcedure(uploaded)
