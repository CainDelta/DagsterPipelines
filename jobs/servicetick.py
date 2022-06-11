import requests
import json
import io
import pandas as pd
from sqlalchemy import create_engine
import time
from dagster import job, op, get_dagster_logger,List,IOManager, io_manager,Out,AssetMaterialization,EventMetadataEntry,AssetKey,daily_partitioned_config,get_dagster_logger,graph,RetryPolicy
import os
import chardet
from pathlib import Path
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
import time
import glob
from datetime import datetime
from dagster import job, op
from dagster_snowflake import snowflake_resource
import random
import numpy as np
import string




local = 'mssql+pyodbc://MIUser:B3nn3tt5!@bendb02.ukfast.bennetts.co.uk/Bennetts_MI?driver=SQL+Server'
snowflake = 'snowflake://BENNETTSDE:Bennetts123!@mg25989.north-europe.azure'

###IO MANAGER
class DataframeTableIOManagerWithMetadata(IOManager):    ##IO manager uploads to SQL database

    def __init__(self, name):
        self.name = name

    def handle_output(self, context, obj):
        sqlcon = create_engine(self.name,fast_executemany=True)
        table_name = context.metadata["table"]
        schema = context.metadata["schema"]
        obj.to_sql(name=table_name, schema=schema,con=sqlcon,if_exists='append',method='multi',index=False, chunksize=8)

        with sqlcon.begin() as conn:
            conn.execute('exec Misc.SP_NPS_Keys')

         # attach these to the Handled Output event
        yield EventMetadataEntry.int(len(obj), label="number of rows")
        yield EventMetadataEntry.text(table_name, label="table name")
    def load_input(self, context):
        sqlcon = create_engine(self.name,fast_executemany=True)
        table_name = context.upstream_output.metadata["table"]
        schema = context.upstream_output.metadata["schema"]
        return read_dataframe_from_table(name=table_name, schema=schema)




@io_manager
def sqlserver_io_manager(_):
    return DataframeTableIOManagerWithMetadata(local)

@io_manager
def snowflake_io_manager(_):
    return DataframeTableIOManagerWithMetadata(snowflake)



##Function Downlaods File
def downloadServiceTick(start):

    print(os.getcwd())
    #os.chdir('./ApiData/api/servicetick')
    os.chdir(r'C:\Users\Arthur.Mubaiwa\Desktop\Bravo\ApiData\api\servicetick')

    ##current dir
    dir = os.getcwd()
    #generate random folder name for each download
    letters = string.ascii_lowercase
    ##make random directory
    temp_folder = ''.join(random.choice(letters) for i in range(10))
    path = dir + '\\downloads\\' + temp_folder + '\\'
    os.makedirs(path, exist_ok=True)


    chromeOptions = webdriver.ChromeOptions()
    # prefs = {'download.default_directory' : 'C:\\Users\\Arthur.Mubaiwa\\Desktop\\Bravo\\ApiData\\api\\servicetick\\downloads\\',
    #         "download.directory_upgrade": True}
    prefs = {'download.default_directory' : path,
            "download.directory_upgrade": True}
    chromeOptions.add_argument('--no-sandbox')
    chromeOptions.add_argument("--headless")
    chromeOptions.add_experimental_option("prefs",prefs)
    # declare and initialize driver variable
    driver=webdriver.Chrome(executable_path="./drivers/chromedriver.exe",options=chromeOptions)

    # options = webdriver.ChromeOptions()
    # options.add_argument("download.default_directory=C:\\Users\\ArthurMubaiwa\\Desktop\\Bravo\\servicetick\\")
    # driver = webdriver.Chrome(executable_path="./servicetick/drivers/chromedriver.exe",chrome_options=options)

    # declare variable to store the URL to be visited
    base_url="https://console.servicetick.com/v2/"
    # declare variable to store search term

    # browser should be loaded in maximized window
    driver.maximize_window()
    # driver should wait implicitly for a given duration, for the element under consideration to load.
    # to enforce this setting we will use builtin implicitly_wait() function of our 'driver' object.
    time.sleep(1) #10 is in seconds
    # to load a given URL in browser window
    driver.get(base_url)
    # test whether correct URL/ Web Site has been loaded or not
    assert "ServiceTick" in driver.title


    ####find username and password boxes
    username=driver.find_element_by_name("Email")
    username.send_keys('chris.gilbert@bennetts.co.uk')

    ##find and send password to element
    password =driver.find_element_by_name("Password")
    password.send_keys('Servicetick123!')

    #Login
    login_button = driver.find_element_by_css_selector('button.btn.btn-lg.btn-primary.btn-block.sign-in')
    login_button.click() # Send mouse click

    ####wait 20 seconds here
    time.sleep(10) #10 is in seconds

    #####ASSERT LOGIN WORKED #########
    # test whether correct URL/ Web Site has been loaded or not
    assert 'ServiceTick | Dashboard' in driver.title


    ##Responses URL
    exports = 'https://console.servicetick.com/v2/Responses/Exports'
    driver.get(exports) ##navigate to export tab
    # test whether correct URL/ Web Site has been loaded or not
    assert 'ServiceTick | Response Exports' in driver.title



    ###click on element
    date_element = driver.find_element_by_id('date-panel-button')
    #date_element.click()
    driver.execute_script("arguments[0].click();", date_element)

    ##click button to enable apply button which is disabled by default
    driver.find_element_by_xpath('//*[@id="ui-id-3"]').click()

    ###FUNCTIONALITY ADDED TO ALLOW DATE SELECTION
    startdate  =  datetime.strptime(start,'%Y-%m-%d')

    start_str = startdate.strftime('%d/%m/%Y')+ ' 00:00:00'
    finish_str = startdate.strftime('%d/%m/%Y')+ ' 23:59:00'

    fromDate = driver.find_element_by_xpath("//*[@id='FromDate']")
    value = driver.execute_script('return arguments[0].value;', fromDate)
    #print("Before update, hidden input value = {}".format(value))
    driver.execute_script('''var elem = arguments[0];var value = arguments[1];elem.value = value;''', fromDate, start_str)

    ToDate = driver.find_element_by_xpath("//*[@id='ToDate']")
    value = driver.execute_script('return arguments[0].value;', ToDate)
    #print("Before update, hidden input value = {}".format(value))
    driver.execute_script('''var elem = arguments[0];var value = arguments[1];elem.value = value;''', ToDate, finish_str)


    apply = driver.find_element_by_id('applyDateButton')
    driver.execute_script("arguments[0].click();", apply)


    ##wait 20 seconds
    time.sleep(10) #10 is in seconds

    export = driver.find_element_by_css_selector('button.btn.btn-primary.floatRight')
    #export.click()
    driver.execute_script("arguments[0].click();", export)

    ##refresh page or wait
    time.sleep(5) #10 is in seconds
    driver.refresh()

    table = driver.find_element_by_css_selector('table.survey-group-stats-table.table-striped')
    row = table.find_element_by_xpath('.//td')


    ###Select downloads
    rwdata = driver.find_elements_by_xpath("//table/tbody/tr[1]/td[5]")

    rwdata[0].click()
    #driver.execute_script("arguments[0].click();", rwdata[0])

    time.sleep(40)
    driver.close()


    return path


@op(config_schema={"date": str})
def runDownload(context) -> str :
    """Download service tick data for specified day"""
    date = context.op_config["date"]
    run = downloadServiceTick(date)
    return run

@op
def readFile(path:str) -> pd.DataFrame:


    """Function reads first file in downloads folder and processes , returns dataframe """
    os.chdir(r'C:\Users\Arthur.Mubaiwa\Desktop\Bravo\ApiData\api\servicetick')

    #path = r'C:\Users\Arthur.Mubaiwa\Desktop\Bravo\ApiData\api\servicetick\downloads\syqwpyylhi'
    ##READ FILE
    files = glob.glob(path + '/*.csv')
    #files = ['./2022_05_12-ResponseExport-22d442ab-bab2-4ead-bb21-bda55523a51a.csv']

    detected = chardet.detect(Path(files[0]).read_bytes())
    encoding = detected.get("encoding")
    service = pd.read_csv(files[0],encoding=encoding)

    return service

@op
def processSurveys(service:pd.DataFrame) -> pd.DataFrame :

    """Rename columns in survey data then check if data exists in db using a filter and return only new rows"""
    ###Map Rename
    col_names = {"We're sorry you were not satisfied with all aspects of our service. Please indicate the areas that require improvement.":"Were sorry you were not satisfied with all aspects of our service. Please indicate the areas that require improvement.",
                "We're delighted you would recommend us. As service is very important to us, please could you tell us what we could do to make it even better?":"Were delighted you would recommend us. As service is very important to us, please could you tell us what we could do to make it even better?",
                "We're sorry you were not satisfied with all aspects of our service. Please indicate the areas that require improvement..1":"Were sorry you were not satisfied with all aspects of our service. Please indicate the areas that require improvement..1",
                "We're delighted you would recommend us. As service is very important to us, please could you tell us what we could do to make it even better?.1":"Were delighted you would recommend us. As service is very important to us, please could you tell us what we could do to make it even better?.1"}


    ##connect to db
    sqlcon = create_engine('mssql+pyodbc://MIUser:B3nn3tt5!@bendb02.ukfast.bennetts.co.uk/Bennetts_MI?driver=SQL+Server')

    db_cols = list(pd.read_sql('select * from Bennetts_MI.Misc.T_Raw_NPS_v2016 ',sqlcon))

    #service = pd.read_csv(r"C:\Users\Arthur.Mubaiwa\Desktop\Bravo\ApiData\api\servicetick\downloads\syqwpyylhi\2022_05_03-ResponseExport-3fa451e7-3339-4ccb-a6f8-3dfaaf66c46c.csv")
    ##Rename with DB columns
    service.rename(columns=dict(zip(service.columns, db_cols)),inplace=True)
    current = pd.read_sql("select * from Misc.T_Raw_NPS_v2016", sqlcon) ##extract current table
    #get_dagster_logger().info(f"Current table has  : {len(current)} rows")


    ##create hash on unique columns, then filter those that exist in the current table and append those that dont
    join_cols = db_cols[:5]
    service['hash'] = service[join_cols].apply(lambda x: hash(tuple(x)), axis=1)
    current['hash'] = current[join_cols].apply(lambda x: hash(tuple(x)), axis=1)
    update = service.loc[~service['hash'].isin(current['hash'])] ##filters out rows that already exist in the transactions
    #get_dagster_logger().info(f"Final Dataframe row number : {len(updates)}")

    update.drop(['hash'], axis=1, inplace=True)

    return update


@op(out=Out(metadata={"schema": "Misc", "table": "T_Raw_NPS_v2016"},io_manager_key="survey_save"))
def uploadSurvey(update:pd.DataFrame):
    #sqlcon = create_engine('snowflake://BENNETTSDE:Bennetts123!@mg25989.north-europe.azure')
    #update.to_sql(name='T_Raw_NPS_v2016', schema='Misc',con=sqlcon,if_exists='append',method='multi',index=False, chunksize=8)
    #update = update[update['Date Time']!='29/04/2022 08:47:34']
    #update.to_sql(name='T_Raw_NPS_v2016', schema='Public',con=sqlcon,if_exists='append',method='multi',index=False, chunksize=8)

    get_dagster_logger().info(f"Final Dataframe row number : {len(update)}")
    return update

###partiton daily
@daily_partitioned_config(start_date="2022-01-01")
def my_partitioned_config(start: datetime, _end: datetime):
    return {"ops": {"runDownload": {"config": {"date": start.strftime("%Y-%m-%d")}}}}
#

@graph
def servicetick():
    getdata = runDownload()
    file = readFile(getdata)
    proc = processSurveys(file)

####RETRY POLICY
retry_policy=RetryPolicy(
        max_retries=3,
        delay=60,
    )


servicetick_job = servicetick.to_job(config=my_partitioned_config,resource_defs={"survey_save": sqlserver_io_manager},op_retry_policy=retry_policy)
servicetick_testing = servicetick.to_job(config=my_partitioned_config,resource_defs={"survey_save": snowflake_io_manager})
