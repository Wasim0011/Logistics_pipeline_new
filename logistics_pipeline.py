from prefect import flow, task
import pandas as pd

# Task 1: Ingest shipment data with Retry Logic (Lab Step 5)
@task(retries=3, retry_delay_seconds=5)
def ingest():
    # dataset: shipments.csv [cite: 22]
    return pd.read_csv("shipments.csv")

# Task 2: Clean missing delivery_time values (Lab Step 1)
@task
def clean(df):
    df["delivery_time"] = df["delivery_time"].fillna(0)
    return df

# Task 3: Calculate average delivery time (Lab Step 1)
@task
def transform(df):
    # delhi, mumbai, bangalore, chennai, kolkata, pune [cite: 23]
    return df.groupby("destination")["delivery_time"].mean().reset_index()

# Task 4: Store processed output (Lab Step 1)
@task
def load(df):
    df.to_csv("output.csv", index=False)

# Pipeline Flow: Ingest -> Clean -> Transform -> Load [cite: 21]
@flow(name="logistics-pipeline")
def logistics_pipeline():
    data = ingest()
    cleaned = clean(data)
    result = transform(cleaned)
    load(result)

if __name__ == "__main__":
    logistics_pipeline()
