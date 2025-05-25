# Serverless Traffic Data Pipeline (AWS/Python)

## Project Title: Building a Monthly NSW Traffic Data Lake for Analytical Insights

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

* **AWS Glue Crawlers** automatically infer the schema from the raw `.csv` files in S3 and register them as tables in the **AWS Glue Data Catalog**. This creates a **centralized, managed metadata repository**, making the raw data immediately discoverable and queryable by other services.
* **Amazon Athena**, a serverless query engine, then performs **SQL-based transformations** directly on these cataloged tables in S3. This **schema-on-read** approach provides immense flexibility.
* Post-transformation, the refined data is written back to a separate S3 location, often in **optimized columnar formats like Apache Parquet or ORC**. This significantly **improves query performance and reduces storage costs** for subsequent analytical workloads.

---

## Architecture Decision: Why Serverless, Why Data Lake?

Choosing a serverless architecture with a data lake approach was a deliberate decision, offering significant advantages for this project:

### Why Serverless Architecture over Traditional Options?

We chose a serverless architecture due to its **reduced operational overhead**, as it eliminated the need to provision or manage servers, allowing us to focus purely on data logic. Its **inherent scalability and elasticity** ensure components like Lambda and S3 automatically handle varying data volumes and loads on demand. This approach is highly **cost-effective** through a pay-per-use model, making it economical for monthly batch jobs. Furthermore, serverless enabled **faster development and deployment**, and facilitated **event-driven automation** via CloudWatch Events, efficiently processing data only when scheduled.

### Why AWS Athena / Glue over a Redshift Data Warehouse?

Instead of a Redshift data warehouse, we opted for **AWS Glue and Athena** for our data lake. This choice was driven by **cost-efficiency**, leveraging Athena's pay-per-query model which is ideal for our intermittent, monthly batch processing. The **schema-on-read flexibility** offered by Glue's Data Catalog and Athena is crucial for handling evolving data structures, while the **separation of compute (Athena) and storage (S3)** provides independent scalability and optimized costs. This approach successfully establishes a **modern data lake foundation**, ensuring raw data remains accessible for diverse analytical tools.

---

## Fully Managed & Robust Architecture:

This pipeline is built entirely on **AWS's fully managed and serverless services**, minimizing operational overhead and maximizing resilience.

For robust operations, **CloudWatch Logs** provide comprehensive **logging and monitoring**, offering insights from application-level details and runtime errors to detailed failure payloads via Lambda Destinations and automatic Lambda metrics for performance. Meanwhile, **AWS IAM** is fundamental for securing the pipeline, strictly managing permissions for each service and adhering to the principle of **least-privilege access** to ensure data security and compliance.

---

## Key Learnings & Challenges:

Through this project, I gained practical experience optimizing **Lambda for cold starts**, meticulously crafting **least-privilege IAM policies** to troubleshoot `403 Forbidden` errors, and ensuring **data integrity** by implementing idempotency checks in Lambda. I also mastered **deployment and packaging** for Python dependencies, effectively utilized AWS Glue Crawlers for **schema discovery and management**, and experienced firsthand the **cost benefits** of a serverless, pay-per-use architecture across all services.
