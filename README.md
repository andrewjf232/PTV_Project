# Serverless Traffic Data Pipeline (AWS/Python)

## Project Goal:

This project focuses on designing and implementing a **fully serverless, scalable ELT (Extract, Load, Transform) pipeline** on AWS. The core objective is to ingest historical traffic congestion data from the TfNSW API, transform it, and deliver **query-ready data** into an Amazon S3-based data lake. This setup enables the generation of **actionable insights** crucial for urban planning and effective traffic management.

In simple terms, this project outlines:

-   ✅ **Where the data comes from** (TfNSW API).
-   ✅ **How it's initially collected** (Python scripts, AWS Lambda, CloudWatch Events).
-   ✅ **Where it's stored** (Amazon S3 Data Lake).
-   ✅ **How it's processed and transformed** (AWS Glue, Amazon Athena).
-   ✅ **How the entire process is automated and controlled** (AWS Lambda, CloudWatch Events, AWS IAM, CloudWatch Logs).

---

## Project Details:

The pipeline collects hourly traffic density data for every traffic station across NSW. This raw information is stored in an **Amazon S3 bucket**, organized using **Hive-style partitioning** (`/year=YYYY/month=MM/day=DD/`) for optimal readability, organization, and query performance.

<img width="807" alt="Screenshot 2025-05-05 at 3 06 08 pm" src="https://github.com/user-attachments/assets/3f55f85e-2f94-422d-bd3e-2760479f26b5" />

---

## Process: A Fully Serverless ELT Pipeline

### Extract & Load

Data extraction and loading are fully automated and managed by AWS serverless services. On the first day of each month, an **AWS CloudWatch Event** triggers an **AWS Lambda function**. This function executes a Python script that makes GET requests to the TfNSW API.
<img width="926" alt="Screenshot 2025-05-25 at 7 24 25 pm" src="https://github.com/user-attachments/assets/4509f95a-e190-4f83-97b4-ef7e63e1b843" />

* The Lambda function ensures that data is not duplicated on re-runs by checking for existing S3 objects before uploading.
* Responses are stored as `.csv` files in the designated S3 bucket, using **Hive-style partitioning**.
* The pipeline includes an initial **backfill** for historical data and then continues with **ongoing monthly batch jobs** for continuous data ingestion.

### Transform
<img width="1172" alt="Screenshot 2025-05-25 at 7 31 02 pm" src="https://github.com/user-attachments/assets/592c1150-9760-4af2-bf1e-e34d81d65fdf" />

* **AWS Glue Crawlers** automatically infer the schema from the raw `.csv` files in S3 and register them as tables in the **AWS Glue Data Catalog**. This creates a **centralised, managed metadata repository**, making the raw data immediately discoverable and queryable by other services.
* **Amazon Athena**, a serverless query engine, then performs **SQL-based transformations** directly on these cataloged tables in S3. This **schema-on-read** approach provides immense flexibility.
* Post-transformation, the refined data is written back to a separate S3 location, often in **optimized columnar formats like Apache Parquet or ORC**. This significantly **improves query performance and reduces storage costs** for subsequent analytical workloads.

### Logging (AWS Cloudwatch)
AWS Lambda automatically sends through logging information that is printed in the console to Cloudwatch. Cloudwatch tracks the logs and also if any errors have been raised. Here you can see that the ETL function ran smoothly without any errors.
<img width="1440" alt="Screenshot 2025-06-10 at 3 58 04 pm" src="https://github.com/user-attachments/assets/9caeb613-2ceb-476c-85c1-4fea3ff96074" />

---

## Architecture Decision: Why Serverless, Why Data Lake?

Choosing a serverless architecture with a data lake approach was a deliberate decision, offering significant advantages for this project:

### Why Serverless Architecture over Traditional Options?

I chose a serverless architecture due to its **reduced operational overhead**, as it eliminated the need to provision or manage servers, allowing us to focus purely on data logic. It was also **cost-effective** through a pay-per-use model. Furthermore, serverless enabled **faster development and deployment**, and allowed for **event-driven automation** via CloudWatch Events.

### Why AWS Athena / Glue over a Redshift Data Warehouse?

Instead of a Redshift data warehouse, I chose **AWS Glue and Athena** for our data lake. This choice was driven by **cost-efficiency**, leveraging Athena's pay-per-query model. The **schema-crawling flexibility** offered by Glue's Data Catalog and Athena is crucial for handling evolving data structures. This approach successfully establishes a **modern data lake foundation**, ensuring raw data remains accessible for diverse analytical tools.


---

## Athena Transformations

### Transformation 1: Joining the station_reference table and hourly_reference tables


A simple JOIN to connect our reference table 'station_reference' containing our station name and other station information, to the actual transport data corresponding to that station in 'hourly_permanent' table. This is essential to translate station_key into the actual name, and details of the station (suburb, road classification, geographical co-ordinates, etc)
<img width="958" alt="Screenshot 2025-06-02 at 3 09 07 pm" src="https://github.com/user-attachments/assets/f570b53a-3a1a-4854-8f94-fa08e9d25545" />

### Transformation 2: Unpivoting the hourly_permanent table
This unpivots the hourlydata so that traffice volume is the same column instead of separate columns. 
i.e. columns "hour_00, hour_01, hour_02 ..." -> column "volume"
#### Before:
<img width="913" alt="Screenshot 2025-06-05 at 12 23 27 pm" src="https://github.com/user-attachments/assets/883291a1-5025-4061-9ec4-5576bbcdb73a" />

#### After:
Here you can see that as the time in the day passes from 0:00am -> 10:00am on Beecroft Road in Sydney on 02-01-2025, the traffic volume increases steadily. 
<img width="1022" alt="Screenshot 2025-06-05 at 12 48 02 pm" src="https://github.com/user-attachments/assets/ac1b8ea8-baf0-4d48-822a-2a862b46e5b0" />
Note: "Volume" is not the total amount of cars passing through. It is a standardised measurement TfNSW provides which allows for a consistent comparison across roads. Actual car counts can be quiet large.

---

## Issues Encountered

### Issue 1: Incorrect Partition Metadata

I encountered an issue where **AWS Glue was incorrectly inferring the `month` column**. Glue was inferring the `month` column from the CSV data itself as type `BIGINT`, instead of recognizing it as a partition key derived from the S3 path (`/month=02/`), which should be a `STRING`.

<img width="1021" alt="Screenshot 2025-06-02 at 3 11 02 pm" src="https://github.com/user-attachments/assets/e2d8e3c9-e460-48ce-82ca-6cc928cb03c0" />

**Resolution:**
AWS Glue had auto-assigned the `month` partition a `BIGINT` type. I renamed the `month` & `year` (type `BIGINT`) columns to `month_data_col` and `year_data_col` and kept separate `month` and `year` (type `string(paritioned)`) columns. This was to avoid duplication of column names, while also helping athena identify which column to keep as the reference to the partitioned key. I had to keep the `BIGINT` columns and not delete them, because when I trid deleting them, it let to issue 2.

---

### Issue 2: Incorrectly Defined `hourly_permanent` Table

The query in `unpivoted_hourly_perm.sql` resulted in some columns not showing data or showing incorrect data. For example, the **`day_of_week` column was showing `'2025'` for every row**, instead of a number between 1 and 7. This indicated that the Glue table defined by our crawler was interpreting the underlying CSVs in S3 incorrectly.

<img width="1047" alt="Screenshot 2025-06-05 at 12 17 16 pm" src="https://github.com/user-attachments/assets/70dc0007-b691-4d08-8d8a-6b527a89dc50" />

**Resolution:**
I **redefined the schema in my Glue table** to ensure it was mapped correctly and accurately representing the underlying data in the correct order. See Issue 1 resolution.

---

## For the Data Analysts:
<img width="794" alt="Screenshot 2025-06-05 at 1 02 52 pm" src="https://github.com/user-attachments/assets/ecc917c3-299b-489d-82f5-a326eff1f826" />

This project transforms raw hourly traffic data, preparing it for powerful analytics. By unpivoting hourly counts and enriching with station details, we create a flexible dataset ideal for:

### Potential Use Cases:

* **Traffic Pattern Analysis:** Understand hourly, daily, and seasonal traffic trends for infrastructure planning.
* **Anomaly Detection:** Identify unusual traffic events for incident response or sensor monitoring.
* **Public Holiday Impact:** Quantify how holidays affect traffic volumes.

