# Serverless Traffic Data Pipeline (AWS/Python)
**Project Title:** AWS Serverless ETL: Building a Monthly NSW Traffic Data Lake for Analytical Insights

**Project Goal:** To design and implement a fully severless, scalable ELT pipeline on AWS for ingesting, transforming and analysing historical traffic congestion data in from the TfNSW API. This pipeline delivers query-ready data into a S3 data lake, which enable actionable insights for urban planning and traffic management needs.

**In simple terms, this project outlines:**

- ✅  **Where the data comes from** (TfNSW API). (DONE)
- ✅  **How it's initially collected** (Python scripts, Boto3 AWS SDK for Python). (DONE)
- ✅ **Where it's stored** (S3 Cloud Storage). (DONE)
- 🟧 **How it's processed and transformed** (Athena). (IN PROGRESS)
- **How the whole process is automated and controlled** (Lambda, cloudwatch events).

  **Project Details:** The data collected will be recording traffic density for each hour of the day in 24 hour time. It will be collecting this data for each traffic station (a location where the NSW government records traffic) located in NSW.
This information is stored in an S3 bucket using Hive-style partioning for readability and also organisational purposes.
<img width="807" alt="Screenshot 2025-05-05 at 3 06 08 pm" src="https://github.com/user-attachments/assets/3f55f85e-2f94-422d-bd3e-2760479f26b5" />

<h1>Process</h1>
<h3>Extract</h3>
Using python, data regarding NSW's traffic will be collected form the TfNSW traffic API over two endpoints. First, a backfill of the last year, and then ongoing monthly batch jobs.
<h3>Load</h3>
This Data will be hive partioned for visbility and organisational purposes (/year=YYYY/month=MM/day=DD/), and stored in an S3 bucket (as .csv files)
<h3>Transform</h3>
Athena will be used inside SageMaker (with CTAS or INSERT INTO) to read the raw data from S3, apply SQL-based transformations. Post-Transformed data will be written back to a different S3 location, often in an optimized format like Parquet or ORC.

<h3>Webapp displaying transport data in NSW using [NiceGUI](https://nicegui.io/#installation)</h3>
This webapp will display transport data in heatmaps with a legend on the side. Heatmap will cover all of NSW Stations. As traffic increases, temperatures will become warmer. 


<h1>MVP To Do</h1>
- Monthly Batch jobs script Separate Python file.
- Reformat Original script to remove daily batch runs.
- Athena transformations via sagemaker.
 - Join hourly_permanent table with station_reference table
 - Identify hourly traffice data with corresponding transport station
 - Top 3 stations with most peak hour traffic (7am - 10am & 3pm - 6pm)
- Apache Airflow (orchestrator)
 - Set up DAGs 
  (Example: 
  Task 1: Run Python script for API extraction. 
  Task 2 (depends on Task 1): Load data to S3. 
  Task 3 (depends on Task 2): Trigger Athena transformation via SageMaker.)
 - Schedule monthly batch jobs
 - Error handling if API is unavailable.
 

<h1>v2.0 To Do</h1>
- Experiment with Sagemaker AI Analysis and ML capabilities. Out of scope for MVP.
