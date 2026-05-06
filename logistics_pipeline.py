from prefect import flow, task
import pandas as pd

# Task: Ingest shipment data with retry logic [cite: 16, 40]
@task(retries=3, retry_delay_seconds=5)
def ingest():
    return pd.read_csv("shipments.csv")

# Task: Clean missing delivery_time values [cite: 17, 31]
@task
def clean(df):
    df["delivery_time"] = df["delivery_time"].fillna(0)
    return df

# Task: Calculate average delivery time [cite: 18, 31]
@task
def transform(df):
    return df.groupby("destination")["delivery_time"].mean().reset_index()

# Task: Store processed output [cite: 19, 31]
@task
def load(df):
    df.to_csv("output.csv", index=False)

# Define the Pipeline Flow [cite: 20, 21]
@flow(name="logistics-pipeline")
def logistics_pipeline():
    data = ingest()
    cleaned = clean(data)
    result = transform(cleaned)
    load(result)

if __name__ == "__main__":
    logistics_pipeline()
