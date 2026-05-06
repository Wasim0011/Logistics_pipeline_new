from prefect import flow, task
import pandas as pd

# Task 1: Ingest with retry logic [cite: 16, 40]
@task(retries=3, retry_delay_seconds=5)
def ingest_data():
    return pd.read_csv("shipments.csv")

# Task 2: Clean data (filling missing values with 0) [cite: 17, 31]
@task
def clean_data(df):
    df["delivery_time"] = df["delivery_time"].fillna(0)
    return df

# Task 3: Transform (calculate delivery metrics) [cite: 18, 31]
@task
def transform_data(df):
    # Grouping by destination and calculating mean delivery time [cite: 31]
    return df.groupby("destination")["delivery_time"].mean().reset_index()

# Task 4: Load processed output [cite: 19, 31]
@task
def load_data(df):
    df.to_csv("output.csv", index=False)

# Define the Pipeline Flow [cite: 21, 31, 38]
@flow(name="Logistics-Pipeline")
def logistics_pipeline():
    raw_data = ingest_data()
    cleaned_data = clean_data(raw_data)
    results = transform_data(cleaned_data)
    load_data(results)

if __name__ == "__main__":
    logistics_pipeline()
