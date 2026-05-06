
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect.exceptions import FailedTask
from prefect.tasks import wait_for
import pandas as pd
import time

@task(retries=3, retry_delay_seconds=5)
def ingest():
    try:
        data = pd.read_csv('shipments.csv')
        return data
    except Exception as e:
        raise FailedTask(f"Failed to read CSV: {e}")

@task
def clean(data):
    data['delivery_time'] = data['delivery_time'].fillna(0)
    return data

@task
def transform(data):
    avg_delivery_time = data.groupby('destination')['delivery_time'].mean().reset_index()
    return avg_delivery_time

@task
def load(data):
    data.to_csv('output.csv', index=False)

@flow
def logistics_pipeline():
    data = ingest()
    cleaned_data = clean(data)
    transformed_data = transform(cleaned_data)
    load(transformed_data)

logistics_pipeline.schedule(cron_expression="0 * * * * *", start_date="2024-01-01")



logistics_pipeline()
