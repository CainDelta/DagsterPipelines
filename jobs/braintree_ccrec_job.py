import requests
import json
import io
import pandas as pd
from sqlalchemy import create_engine
import time
from dagster import job, op, get_dagster_logger,List,IOManager, io_manager,Out,AssetMaterialization,EventMetadataEntry,AssetKey,daily_partitioned_config,get_dagster_logger,monthly_partitioned_config
import braintree
import datetime
import certifi
import pyodbc
from sqlalchemy import create_engine
import os
import urllib3
import urllib.parse
import numpy as np
from dateutil.relativedelta import relativedelta
from credentials import dbstring,merchant_id,public_key,private_key

###IO MANAGER
class DataframeTableIOManagerWithMetadata(IOManager):



    ##IO manager uploads to SQL database
    def handle_output(self, context, obj):
        sqlcon = create_engine(dbstring,fast_executemany=True)
        table_name = context.metadata["table"]
        schema = context.metadata["schema"]
        obj.to_sql(name=table_name, schema=schema,con=sqlcon,if_exists='append',method='multi',index=False, chunksize=50)

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



##Convert numbers to negative
def negativeNumber(x):
    neg = x * (-1)
    return neg





@op
def instantiateBraintree():

    gateway = braintree.BraintreeGateway(
              braintree.Configuration(
                   environment=braintree.Environment.Production,
                   merchant_id = merchant_id,
                   public_key = public_key,
                   private_key= private_key
              )
            )

    return gateway


##LOOK TO ADDING DATE AS A PARAMETER TO BACKFILL ---- LONG TERM
@op (config_schema={"date": str})
def generateTransactions(context,gateway) -> List :
        """Uses date to get transactions from Gateway """


        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        ##date
        #start = datetime.datetime.today()-datetime.timedelta(days=2)
        #date_end = date + datetime.timedelta(days=1)
        start = context.op_config["date"]

        #start = '2021-06-08'
        date_start = datetime.datetime.strptime(start,'%Y-%m-%d')
        date_end = date_start + datetime.timedelta(days=1)


        get_dagster_logger().info(f"Date Range from : {date_start} to : {date_end}")
        #context.add_output_metadata({"foo": "bar"}) ---dagster 0.14
        collection = gateway.transaction.search(
            braintree.TransactionSearch.created_at.between(date_start,date_end)
        )

        transactions = [transaction for transaction in collection.items]

        return transactions


@op (config_schema={"date": str})
def generateTransactionsMonthly(context,gateway) -> List :
        """Uses date to get transactions from Gateway """


        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        ##date
        #start = datetime.datetime.today()-datetime.timedelta(days=2)
        #date_end = date + datetime.timedelta(days=1)
        start = context.op_config["date"]
        #start = '2021-06-08'
        date_start = datetime.datetime.strptime(start,'%Y-%m-%d')
        date_end = date_start + relativedelta(day=31) ##relativdelta gets end of the month


        get_dagster_logger().info(f"Date Range from : {date_start} to : {date_end}")
        #context.add_output_metadata({"foo": "bar"}) ---dagster 0.14
        collection = gateway.transaction.search(
            braintree.TransactionSearch.created_at.between(date_start,date_end)
        )

        transactions = [transaction for transaction in collection.items]

        return transactions

##### PROCESS TRANSACTIONS
@op
def processTransactions(transactions:List) -> pd.DataFrame :

    """Breaks down list of transactions and selects key fields """

    trans = []
    for t in transactions:

        ##Get Masked card number
        try:
            masked = t.credit_card['bin'] + '******' + t.credit_card['last_4']
        except Exception as e:
            pass
            masked = None
        ##Get settlement date or null if none
        if (t.status_history[len(t.status_history)-1].status == 'settled'):
            settlement_date = t.status_history[len(t.status_history)-1].timestamp
        else:
            settlement_date = None

        ####Paypal transaction fee
        try:
            paypal_transaction_fee = t.paypal_details.transaction_fee_amount
        except Exception as e:
            pass
            paypal_transaction_fee = None

        t.global_id
        trans.append((t.id,t.order_id,t.merchant_account_id,t.status,t.created_at,t.authorized_transaction_id,t.amount,t.refunded_transaction_id,t.refund_id,settlement_date,t.credit_card['card_type'],masked,
        t.gateway_rejection_reason,t.customer['first_name'],t.customer['last_name'],t.authorized_transaction_global_id,t.processor_authorization_code,t.processor_response_code, t.processor_response_text,t.payment_instrument_type,t.service_fee_amount,paypal_transaction_fee,t.tax_amount))

            ##CReate Table
    orders = pd.DataFrame(trans,columns=['id','order_id','merchant_account_id','status','created_at','authorized_transaction_id','amount','refunded_transaction_id','refund_id','settlement_date','credit_card_type','masked_cc_number',
    'gateway_rejection_reason','customer_first_name','customer_last_name','authorized_transaction_global_id','processor_authorization_code','processor_response_code', 'processor_response_text','payment_instrument_type','service_fee_amount','paypal_transaction_fee','tax_amount'])



    get_dagster_logger().info(f"Downloaded {len(transactions)} transactions")
    get_dagster_logger().info(f"Orders row number : {len(orders)}")

    return orders


##FILTER AND ADJUST
@op(out={'table_out':Out(metadata={"schema": "Misc", "table": "T_BrainTree_Transactions"},io_manager_key="orders_save"),'table_in': Out(pd.DataFrame)})
def transformData(orders:pd.DataFrame) :

    """ Add columns check amounts, and upload to Bennetts_MI"""

    #sqlcon = create_engine(dbstring,fast_executemany=True)

    orders['row_id'] = orders.id.fillna('') + '-' + orders.order_id.fillna('') + '-' + orders.status #create unique row_id and fills in na with blank space
    ##Change Type
    orders = orders.convert_dtypes()
    orders['amount'] = orders['amount'].astype(float)

    ##Convert Refunds to negatives
    orders['amount'] = orders.apply(lambda x: x['amount'] if pd.isnull(x['refunded_transaction_id']) else negativeNumber(x['amount']),axis=1)
    #orders['amount'] = orders.apply(lambda x: x['amount'] if pd.isnull(x['refund_id']) else negativeNumber(x['amount']),axis=1)
    orders.to_csv('BraintreeColumns.csv',index=False)

    #get_dagster_logger().info(f"Orders row number : {len(orders)}")
    ##Manual Upsert rows into Transactions table
    sqlcon = create_engine(dbstring,fast_executemany=True)

    current = pd.read_sql("select * from [Bennetts_MI].[Misc].[T_BrainTree_Transactions]", sqlcon) ##extract current table
    get_dagster_logger().info(f"Current table has  : {len(current)} rows")

    updates = orders[~orders.row_id.isin(current.row_id.to_list())] ##filters out rows that already exist in the transactions
    get_dagster_logger().info(f"Final Dataframe row number : {len(updates)}")
    #orders.to_sql(name='T_BrainTree_Transactions_Test',schema='Misc',index=False,con=sqlcon,if_exists='append',method='multi', chunksize=50) ##update
    #updates.to_sql(name='T_BrainTree_Transactions',schema='Misc',index=False,con=sqlcon,if_exists='append',method='multi', chunksize=100) ##update




    return updates,updates

###TRANSFORM MONTHLY
@op(out=Out(metadata={"schema": "Testing", "table": "T_BrainTree_Response_Codes"},io_manager_key="orders_save"))
def transformDataMonthly(orders:pd.DataFrame) :

    """ Add columns check amounts, and upload to Bennetts_MI"""

    #sqlcon = create_engine(dbstring,fast_executemany=True)

    orders['row_id'] = orders.id.fillna('') + '-' + orders.order_id.fillna('') + '-' + orders.status #create unique row_id and fills in na with blank space
    ##Change Type
    orders = orders.convert_dtypes()
    orders['amount'] = orders['amount'].astype(float)

    ##Convert Refunds to negatives
    orders['amount'] = orders.apply(lambda x: x['amount'] if pd.isnull(x['refunded_transaction_id']) else negativeNumber(x['amount']),axis=1)


    #get_dagster_logger().info(f"Orders row number : {len(orders)}")
    ##Manual Upsert rows into Transactions table
    sqlcon = create_engine(dbstring,fast_executemany=True)

    current = pd.read_sql("select * from [Bennetts_MI].Testing.T_BrainTree_Response_Codes", sqlcon) ##extract current table
    get_dagster_logger().info(f"Current table has  : {len(current)} rows")

    ##SUBSET COLUMNS
    orders = orders[list(current)] #take only columns in testing table

    updates = orders[~orders.id.isin(current.id.to_list())] ##filters out rows that already exist in the transactions
    get_dagster_logger().info(f"Final Dataframe row number : {len(updates)}")
    #orders.to_sql(name='T_BrainTree_Transactions_Test',schema='Misc',index=False,con=sqlcon,if_exists='append',method='multi', chunksize=50) ##update
    #updates.to_sql(name='T_BrainTree_Transactions',schema='Misc',index=False,con=sqlcon,if_exists='append',method='multi', chunksize=100) ##update


    return updates,updates


@op
def runStoredProcedure(table_in:pd.DataFrame):
    """Run CCRec Procedure"""
    sqlcon = create_engine(dbstring,fast_executemany=True)

    with sqlcon.begin() as conn:
        conn.execute('EXECUTE [Policies].[SP_CreditCard_Rec_Settlements]')
        conn.execute('EXECUTE [Braintree].[SP_Braintree_Failed_Payments]')
        conn.execute('EXECUTE [Policies].[SP_CreditCard_Rec_SLTRANS_2020]')


###daily partition
@daily_partitioned_config(start_date="2021-06-01")
def my_partitioned_config(start: datetime, _end: datetime):
    return {"ops": {"generateTransactions": {"config": {"date": start.strftime("%Y-%m-%d")}}}}
#
#monthly partiton
@monthly_partitioned_config(start_date="2021-06-01")
def monthly_partition(start: datetime, _end: datetime):
    return {"ops": {"generateTransactionsMonthly": {"config": {"date": start.strftime("%Y-%m-%d")}}}}


###DAILY PARTITON JOB
@job(config=my_partitioned_config,resource_defs={"orders_save": df_table_io_manager})
def execute_CCRec():
    gc = instantiateBraintree()
    trans_list = generateTransactions(gc)
    orders = processTransactions(trans_list)
    table_out,table_in = transformData(orders)
    runStoredProcedure(table_in)

###MONTHLY PARTITION JOB
@job(config=monthly_partition,resource_defs={"orders_save": df_table_io_manager})
def execute_CCRec_MonthlyBackfill():
    gc = instantiateBraintree()
    trans_list = generateTransactionsMonthly(gc)
    orders = processTransactions(trans_list)
    transformDataMonthly(orders)
