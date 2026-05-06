from prefect import flow, task
import pandas as pd

# Task 1: Ingest shipment data with Retry Logic [cite: 40]
@task(retries=3, retry_delay_seconds=5)
def ingest():
    return pd.read_csv("shipments.csv")

# Task 2: Clean missing delivery_time values [cite: 17, 28]
@task
def clean(df):
    df["delivery_time"] = df["delivery_time"].fillna(0)
    return df

# Task 3: Calculate delivery metrics per destination [cite: 18, 28]
@task
def transform(df):
    return df.groupby("destination")["delivery_time"].mean().reset_index()

# Task 4: Store processed output [cite: 19, 28]
@task
def load(df):
    df.to_csv("output.csv", index=False)

# Define the Pipeline Flow (DAG) [cite: 4, 31]
@flow(name="logistics_pipeline")
def logistics_pipeline():
    data = ingest()
    cleaned = clean(data)
    result = transform(cleaned)
    load(result)

if __name__ == "__main__":
    logistics_pipeline()
