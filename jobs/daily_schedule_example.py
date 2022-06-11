from dagster import (
    build_schedule_from_partitioned_job,
    daily_partitioned_config,

    job,
    op,
    repository,
)
import datetime

@daily_partitioned_config(start_date='2020-01-01')
def my_partitioned_config(start: datetime, _end: datetime):
    return {"ops": {"process_data_for_date": {"config": {"date": start.strftime("%Y-%m-%d")}}}}


@op(config_schema={"date": str})
def process_data_for_date(context):
    return context.op_config["date"]


@job(config=my_partitioned_config)
def do_stuff_partitioned():
    process_data_for_date()
