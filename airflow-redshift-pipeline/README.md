# Data Warehouse Pipeline with Airflow & Redshift

## 📚 Project Overview

This project builds a data pipeline using **Apache Airflow**, **Amazon Redshift**, and **Amazon S3**. It loads raw JSON data from S3 into Redshift, transforms it into analytics-friendly star schema tables, and verifies data quality using custom checks.

---

## 📁 Project Structure

.
├── dags/
│   └── final_project_dag.py             # Main DAG definition
├── plugins/
│   ├── operators/
│   │   ├── stage_redshift.py           # Stage data from S3 to Redshift
│   │   ├── load_dimension.py           # Load dimension tables
│   │   ├── load_fact.py                # Load fact table
│   │   └── data_quality.py             # Run data quality checks
│   └── sql/
│       └── sql_statements.py           # SQL queries (e.g. COPY, CREATE, INSERT)
└── README.md



---

## 🗂️ Data Sources

- **S3**:
  - `s3://<your-bucket>/log-data/` – Event logs
  - `s3://<your-bucket>/song-data/` – Song metadata

---

## 🧱 Tables Overview

### Staging Tables
- `staging_events`
- `staging_songs`

### Fact Table
- `songplays`

### Dimension Tables
- `users`
- `songs`
- `artists`
- `time`

---

## ⚙️ Workflow Steps

1. **Create Tables** – Executes `create_tables.sql` in Redshift
2. **Stage Data** – Copies JSON files from S3 to `staging_*` tables
3. **Load Fact Table** – Inserts into `songplays` with joins
4. **Load Dimensions** – Populates dimension tables with distinct values
5. **Run Quality Checks** – Verifies data integrity (e.g., non-empty, no NULL keys)

---

## 🧩 Requirements

- Python ≥ 3.7
- Apache Airflow ≥ 2.0
- AWS account with Redshift and S3 access
- Airflow Connections:
  - `aws_credentials` (type: Amazon Web Services)
  - `redshift` (type: Postgres)

---

## 🚀 Usage

> **Notice:** Ensure your Airflow connections (`aws_credentials`, `redshift`) are configured before running the DAG.

1. **Initialize the Airflow metadata database**  
   `airflow db init`

2. **Start the scheduler**  
   `airflow scheduler`

3. **Start the web server**  
   `airflow webserver`

4. **Trigger the DAG**  
   - In your browser, go to http://localhost:8080  
   - Switch the `final_project` DAG slider to “on”  
   - Click the ▶️ (run) button next to the latest scheduled run  


## 🔐 Notes

- Use temporary AWS Access & Secret Keys for development only. Prefer IAM roles in production.  
- Redshift and S3 should be in the same AWS region.  
- `COPY` command uses `FORMAT AS JSON 'auto'` or a custom path.  
- Redshift user must have `COPY` permissions and access to referenced S3 bucket.  

## 📜 License

This project is part of the Udacity Data Engineering Nanodegree.  

