import os
from sendgrid import SendGridAPIClient
import json
import pandas as pd
from credentials import dbstring,sendgrid_api
os.getcwd()
sg = SendGridAPIClient(sendgrid_api)
params = {'start_date': '2022-05-22'}
response = sg.client.stats.get(
    query_params=params
)
print(response.status_code)
print(response.body)
print(response.headers)
Stats = json.loads(response.body)
##for x in Stats:
##    print(x.keys())
Statslist= []
for x in Stats:
    date =x['date']    ## this brings a single value for the date value
    data =x['stats'][0]['metrics']  ##key stats brought back a one element list and inside list was dictionary and indexed on the first element and then brought back everything in metrics
    df = pd.DataFrame([data])
    df['date'] = date ## created a new field and gave it a value date and then brought them together.
    Statslist.append(df)
GlobalStats = pd.concat(Statslist).reset_index(drop=True)
#when they unsubscribe



import datetime
startdate = datetime.datetime.timestamp(datetime.datetime.strptime('2022-05-22','%Y-%m-%d'))
enddate = datetime.datetime.timestamp(datetime.datetime.strptime('2022-05-31','%Y-%m-%d'))


x = list(range(0,20000,500))
x
params = {'start_time': int(startdate), 'end_time': int(enddate),'offset':0}
response = sg.client.suppression.unsubscribes.get()
a = json.loads(response.body)
b = pd.DataFrame(a)


print(response.body)

#block list
import datetime
startdate = datetime.datetime.timestamp(datetime.datetime.strptime('2022-05-22','%Y-%m-%d'))
enddate = datetime.datetime.timestamp(datetime.datetime.strptime('2022-05-31','%Y-%m-%d'))
#enddate
params = {'start_time': int(startdate), 'end_time': int(enddate)}
response = sg.client.suppression.blocks.get(query_params=params)
print(response.status_code)
print(response.body)
