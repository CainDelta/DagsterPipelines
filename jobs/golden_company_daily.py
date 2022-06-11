import requests
import json
import io
import pandas as pd
from sqlalchemy import create_engine
import time
from dagster import job, op, get_dagster_logger,IOManager, io_manager,AssetMaterialization,EventMetadataEntry,Out,AssetKey
from GC.GetData import  GoldenCompany
import os
import datetime
# class DataframeTableIOManagerWithMetadata(IOManager):
#     def handle_output(self, context, obj):
#         table_name = context.name
#         #write_dataframe_to_table(name=table_name, dataframe=obj)
#         #obj.write_csv("./data/squad")
#         # attach these to the Handled Output event
#         yield EventMetadataEntry.int(len(obj), label="number of rows")
#         yield EventMetadataEntry.text(table_name, label="table name")
#
#     def load_input(self, context):
#         table_name = context.upstream_output.name
#         #return read_dataframe_from_table(name=table_name)
#         return read_csv("some/path")


###MTERIALIZATION TO LOCAL FILE
class PandasCsvIOManagerWithAsset(IOManager):
    def load_input(self, context):
        file_path = os.path.join("./GC/", context.step_key, context.name)
        return read_csv(file_path)

    def handle_output(self, context, obj):


        dt = datetime.datetime.now().strftime("%d-%m-%Y_%H%M")
        file_path = os.path.join(r'C:\Users\Arthur.Mubaiwa\Desktop\Bravo\ApiData\Dagster\GC', 'squad_data')
        file_name = 'squad_'+dt+'.csv'
        full_path = os.path.join(file_path,file_name)

        try:
            os.makedirs(file_path)
        except OSError as e:
            print('Error raised')

        obj.to_csv(full_path)

        yield AssetMaterialization(
            asset_key=AssetKey(full_path),
            description="Persisted result to storage.",
            metadata={
                "number of rows": obj.shape[0]
                #"some_column mean": obj["some_column"].mean(),
            },
        )

##
@io_manager
def save_table_io_manager(_):
    return PandasCsvIOManagerWithAsset()


##initiate GoldenCompany instance
@op
def initiateGC():
    gc = GoldenCompany()
    return gc


## Return player dataframe
@op(out=Out(io_manager_key="squad_csv"))
def getPlayerData(gc) -> pd.DataFrame:

    squad_data = gc.playerData
    #remote_storage_path = persist_to_storage(squad_data)
    #yield AssetMaterialization(asset_key="my_dataset", description="Persisted result to storage")
    #yield Output(remote_storage_path)

    return squad_data


@job(resource_defs={"squad_csv": save_table_io_manager})
def golden_company_daily_job():
    gc = initiateGC()
    getPlayerData(gc)
