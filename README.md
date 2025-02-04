# MANUAL Health Retention Analysis - dbt & BigQuery Setup

## 📌 Overview
This dbt project models customer retention for MANUAL Health, analyzing:
- **Cohort-Based Retention (90-Day Retention)**
- **Month-over-Month Retention**
- **Subscription Tenure Analysis**

## 🛠️ Setup Guide

### 1️⃣ Install Python & dbt
Ensure Python and dbt are installed:

```sh
# Install Python 3.9+
brew install python  # (Mac)
sudo apt install python3-pip  # (Linux)

# Install dbt (BigQuery adapter)
pip install dbt-bigquery
```

### 2️⃣ Configure dbt to Connect to BigQuery
1. Authenticate with Google Cloud:
```sh
gcloud auth application-default login
```
2. Create a `profiles.yml` file (usually in `~/.dbt/`):
```yaml
manual_health:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: oauth
      project: <your-bigquery-project-id>
      dataset: manual_health_dbt
      threads: 4
      timeout_seconds: 300
      location: US
      priority: interactive
```
3. Test the connection:
```sh
dbt debug
```

### 3️⃣ Run dbt Models
```sh
# Create and run models
dbt run
# Test the output
dbt test
```

---

## 📊 dbt Models & Use Cases

| **Model Name** | **Description** |
|---------------|----------------|
| `subscriptions` | Standardized subscription data for all retention models. |
| `cohort_retention` | 90-day retention: Tracks if a customer is still active after 90 days. |
| `retention_month_over_month` | Month-over-month retention: Checks if a customer remains active in the next month. |
| `tenure_analysis` | Calculates average subscription tenure by business category and country. |

---


