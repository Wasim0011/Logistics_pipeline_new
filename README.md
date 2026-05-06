# 🚀 AI-Driven Logistics Data Pipeline

An automated data engineering pipeline that utilizes **Grok API (xAI)** to generate a **Prefect 3.0 DAG**, orchestrated via **GitHub Actions** for daily logistics shipment processing.

---

## 📌 Project Overview

This project automates the transition from **natural language pipeline descriptions** to **production-ready orchestration code**. It processes daily shipment data for a logistics company and calculates key delivery performance metrics.

---

## 🏗️ Pipeline Flow (Medallion Architecture)

The pipeline follows a structured data engineering workflow:

### 🔹 Ingest

* Reads raw `shipments.csv` data
* Includes built-in retry logic for fault tolerance

### 🔹 Clean

* Handles missing values
* Standardizes data formats

### 🔹 Transform

* Calculates average delivery times per destination
* Example cities: Delhi, Mumbai, Bangalore

### 🔹 Load

* Stores final analytical results in `output.csv`

---

## 🛠️ Tech Stack

* **Orchestrator:** Prefect 3.0 (Pythonic DAGs)
* **Data Processing:** Pandas
* **AI Engine:** Grok API (xAI)
* **Automation:** GitHub Actions (CI/CD & Scheduling)

---

## ⚙️ Features

* 🤖 **Automated Code Generation**

  * AI-generated pipeline structure reduces manual boilerplate

* 🔁 **Robust Error Handling**

  * Retry logic: *3 retries with 5-second delay*

* ⏰ **Automated Scheduling**

  * Runs daily via GitHub Actions (crontab)

* 🚀 **CI/CD Deployment**

  * Push-to-run automation
  * Generates CSV artifacts directly in the repository

---

## 📂 File Structure

```
.
├── logistics_pipeline.py        # Core Prefect DAG script
├── shipments.csv               # Raw logistics dataset
├── requirements.txt            # Python dependencies
└── .github/
    └── workflows/
        └── main.yml            # GitHub Actions workflow
```

---

## 🚀 Getting Started

### ✅ Prerequisites

* Python 3.10+
* Grok API Key (for code generation)
* Prefect Cloud or Local Instance

---

### ▶️ Local Execution

#### 1. Clone the Repository

```bash
git clone https://github.com/Wasim0011/Logistics_pipeline_new.git
cd Logistics_pipeline_new
```

#### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

#### 3. Run the Pipeline

```bash
python logistics_pipeline.py
```

---

## ⚙️ GitHub Actions Setup

To automate execution in your own repository:

1. Navigate to:
   **Settings → Secrets and variables → Actions**

2. Add a new repository secret:

```
Name: XAI_API_KEY
Value: GROK_API_KEY
```

---

## 📊 Sample Data

Example dataset processed by the pipeline:

| shipment_id | origin    | destination | status    | delivery_time |
| ----------- | --------- | ----------- | --------- | ------------- |
| 1           | Delhi     | Mumbai      | Delivered | 2             |
| 2           | Bangalore | Chennai     | Delivered | 1             |
| 3           | Delhi     | Kolkata     | Pending   | 4             |

---

## 🧪 Lab Exercises Included

* **Exercise 1:** Add dynamic `shipment_cost` column
* **Exercise 2:** Integrate fraud detection logic into DAG
* **Exercise 3:** Simulate failures and test retry logic

---

## 👨‍💻 Author

**Md. Wasim**
*Data Engineering Intern | NIT Raipur*

---

## ⭐️ Show Your Support

If you found this project useful, consider giving it a ⭐️ on GitHub!
