from dagster_snowflake import snowflake_resource
from dagster import job, op

##########SNOWFLAKE ####################

#####snowflake resource #######
snowflake_res =  {
                'config': {
                    'account':'mg25989.north-europe.azure',
                    'user': 'BENNETTSDE',
                    'password':'Bennetts123!',
                    'database': 'MI',
                    'schema': 'PUBLIC',
                    'warehouse':'COMPUTE_WH',
                }
            }



@op(required_resource_keys={'snowflake'})
def get_one(context):
    context.resources.snowflake.execute_query('SELECT 1')

@job(resource_defs={'snowflake': snowflake_resource})
def my_snowflake_job():
    get_one()


#
import snowflake.connector

# Gets the version
ctx = snowflake.connector.connect(
    user='BENNETTSDE',
    password='Bennetts123!',
    account='mg25989.north-europe.azure'
    )
cs = ctx.cursor()
try:
    cs.execute("SELECT current_version()")
    one_row = cs.fetchone()
    print(one_row[0])
finally:
    cs.close()
ctx.close()

########################
engine = create_engine('snowflake://BENNETTSDE:Bennetts123!@mg25989.north-europe.azure')
try:
    connection = engine.connect()
    results = connection.execute('select current_version()').fetchone()
    print(results[0])
finally:
    connection.close()
    engine.dispose()



#####snowflake resource #######
snowflake_res =  {
                'config': {
                    'account':'mg25989.north-europe.azure',
                    'user': 'BENNETTSDE',
                    'password':'Bennetts123!',
                    'database': 'MI',
                    'schema': 'PUBLIC',
                    'warehouse':'COMPUTE_WH',
                }
            }



# my_snowflake_job.execute_in_process(
#     run_config={
#         'resources': {
#             'snowflake': snowflake_res}})

if __name__ == '__main__':
    result = my_snowflake_job.execute_in_process(
        run_config={
            'resources': {
                'snowflake': snowflake_res}})
