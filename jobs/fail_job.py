from random import random

from dagster import in_process_executor, job, op,RetryPolicy


@op
def start():
    return 1


@op
def unreliable(num: int) -> int:
    failure_rate = 0.5
    if random() < failure_rate:
        raise Exception("blah")

    return num


@op
def end(_num: int):
    pass

retry_policy=RetryPolicy(
        max_retries=3,
        delay=2,  # 200ms
        # backoff=Backoff.EXPONENTIAL,
        # jitter=Jitter.PLUS_MINUS,
    )

@job(op_retry_policy=retry_policy)
def unreliable_job():
    end(unreliable(start()))
