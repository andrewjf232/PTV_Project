# NSW Traffic Congestion Data ELT - Data Engineering 
**Project Title:** Traffic in NSW - Congestion Study (2024 - 2025)

**Project Goal:** To collect traffic data from the TfNSW API and perform an end-to-end extraction, loading and transformation of the data. The output will be 12 months of traffic data in NSW loaded in an Amazon S3 bucket, and transformed in a Google BigQuery Warehouse. This data will be analysed and also be publicly available.

**In simple terms, this project outlines:**

- ✅  **Where the data comes from** (TfNSW API). ✅  (DONE)
- ✅  **How it's initially collected** (Python scripts). ✅ (DONE)
- **Where it's stored** (S3 Cloud Storage, BigQuery Data Warehouse Staging/Modeled areas). (IN PROGRESS)
- **How it's processed and transformed** (dbt Models).
- **How the whole process is automated and controlled** (Airflow triggers).

  **Project Details:** The data collected will be recording traffic density for each hour of the day in 24 hour time. It will be collecting this data for each traffic station (a location where the NSW government records traffic) located in NSW.
This information is stored in an S3 bucket using Hive-style partioning for readability and also organisational purposes.
<img width="807" alt="Screenshot 2025-05-05 at 3 06 08 pm" src="https://github.com/user-attachments/assets/3f55f85e-2f94-422d-bd3e-2760479f26b5" />
