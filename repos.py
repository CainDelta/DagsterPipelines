import sys
sys.path.insert(1, './jobs/') ##add folder to path

from dagster import repository
from jira_testing import executeJIRA
from calabrio_job import Calabrio_Job
from hello_cereal import hello_cereal_job
from complex_job import diamond
from Sitecore_Marketing_Processing import sitecore_analytics
from dagster import ScheduleDefinition,build_schedule_from_partitioned_job
from golden_company_daily import golden_company_daily_job
from braintree_ccrec_job import execute_CCRec,execute_CCRec_MonthlyBackfill
from servicetick import servicetick_job
from servicetick import servicetick_testing
from lapsedpayments import lapsed_payments
from twilio_abandoned_calls import Twilio_Abandoned_Calls
from twilio_webchat import Twilio_Webchat_CSAT
from twilio_activities import Twilio_Activities_Job
from Aquotes import Raw_AQuotes_Batch
from aquotes_dbt import aquotes_dbt_models
from twilio_calls import Twilio_Calls_Job
from fail_job import unreliable_job
# from Sitecore_DBT_Stages import Sitecore_DBT
#from assets import all_assets


##Schedules
calabrio_schedule = ScheduleDefinition(job=Calabrio_Job, cron_schedule="0 4 * * *")
jira_schedule = ScheduleDefinition(job=executeJIRA, cron_schedule="30 4 * * *")
golden_schedule = ScheduleDefinition(job=golden_company_daily_job, cron_schedule="30 4 * * *")
twilio_abandoned_schedule = ScheduleDefinition(job=Twilio_Abandoned_Calls, cron_schedule="30 4 * * *")
twilio_csat_schedule = ScheduleDefinition(job=Twilio_Webchat_CSAT, cron_schedule="30 4 * * *")
#calabrio_schedule = ScheduleDefinition(job=executeJIRA, cron_schedule="0 4 * * *")
braintree_schedule = build_schedule_from_partitioned_job(job=execute_CCRec, description='Braintree loading schedule', minute_of_hour=10, hour_of_day=4)
servicetick_schedule = build_schedule_from_partitioned_job(job=servicetick_job, description='Service loading schedule', minute_of_hour=10, hour_of_day=4)
aquotes_schedule = build_schedule_from_partitioned_job(job=Raw_AQuotes_Batch, description='Aquotes loading schedule', minute_of_hour=10, hour_of_day=4)


@repository
def API_Repository():


    return [executeJIRA, Calabrio_Job,hello_cereal_job,diamond,calabrio_schedule,jira_schedule,golden_company_daily_job,golden_schedule,execute_CCRec,braintree_schedule,execute_CCRec_MonthlyBackfill,sitecore_analytics,
    servicetick_job,servicetick_schedule,Twilio_Webchat_CSAT,Twilio_Abandoned_Calls,twilio_csat_schedule,twilio_abandoned_schedule,Twilio_Activities_Job,Raw_AQuotes_Batch,aquotes_schedule,Twilio_Calls_Job]

    # }
@repository
def Testing_Repo():
    return [servicetick_testing,unreliable_job]

@repository
def MonthlyRepo():
    return [lapsed_payments]

@repository
def dbtModels():
    return [aquotes_dbt_models]
