# PTV_Project
**Project Title:** Real-time PTV Transit Performance Analysis Pipeline

**Project Goal:** To build an automated data pipeline that ingests PTV static schedules and real-time vehicle updates, processes and models the data to analyze route performance and punctuality (potentially focusing on services around Hoppers Crossing/Wyndham area), and stores it in a data warehouse.

**In simple terms, this project outlines:**

- **Where the data comes from** (PTV API, GTFS Feed).
- **How it's initially collected** (Python scripts).
- **Where it's stored** (Cloud Storage, Data Warehouse Staging/Modeled areas).
- **How it's processed and transformed** (dbt Models).
- **How the whole process is automated and controlled** (Airflow triggers).
