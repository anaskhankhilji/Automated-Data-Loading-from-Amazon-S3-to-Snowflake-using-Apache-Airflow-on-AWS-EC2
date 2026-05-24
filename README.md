# Automated-Data-Loading-from-Amazon-S3-to-Snowflake
An end-to-end Data Engineering pipeline that automates loading CSV data from AWS S3 into Snowflake using Apache Airflow on EC2. The workflow includes S3 file sensing, automatic table creation, data ingestion, and email notifications for reliable and scalable cloud-based ETL orchestration.
### Architecture Diagram
<img width="1095" height="568" alt="Untitled Diagram drawio" src="https://github.com/user-attachments/assets/adfec79d-f7bd-487f-9dd5-4ec0eabde09e" />

### Workflow Overview

The pipeline follows the below sequence:

Data files are uploaded into an Amazon S3 bucket.

Apache Airflow running on an AWS EC2 instance monitors the bucket.

The S3KeySensor checks whether the target file exists.

Once detected, Airflow executes SQL queries in Snowflake.

A Snowflake table is automatically created if not present.

CSV data is copied from S3 into Snowflake tables.

Pipeline execution status is logged inside Airflow.

Optional email notifications can be configured for success/failure alerts.

### Key Features
Fully automated ETL/ELT pipeline
Real-time file detection using Airflow Sensors
Scalable cloud architecture
Integration between AWS S3 and Snowflake
Workflow orchestration using Apache Airflow
Automated Snowflake table creation
Secure credential management
Task dependency handling
Retry and failure management
Monitoring and logging support
Modular DAG design
Production-style cloud deployment

### Technologies Used
Cloud Services

Amazon Web Services (AWS)
EC2
S3
IAM
Data Engineering Tools
Apache Airflow
Snowflake Data Warehouse
Programming & Query Languages
Python
SQL
Airflow Operators Used
S3KeySensor
SQLExecuteQueryOperator

### Project Components
1. Apache Airflow on EC2

Apache Airflow is deployed on an AWS EC2 instance to orchestrate the entire workflow. Airflow manages task scheduling, dependencies, retries, monitoring, and execution logs.

The DAG includes:

Sensor tasks
SQL execution tasks
Data loading tasks
Dependency management

### 2. Amazon S3

Amazon S3 acts as the raw data storage layer where CSV files are uploaded before processing.

Responsibilities:

Store incoming CSV files
Serve as source storage for ingestion
Trigger downstream pipeline execution

### 3. Snowflake Data Warehouse

Snowflake is used as the cloud-based analytical data warehouse where structured data is loaded for analytics and reporting.

Responsibilities:

Store processed structured datasets
Support analytical workloads
Enable scalable querying and reporting

### Airflow DAG Workflow
Task 1 — Check File Availability
tsk_is_file_in_s3_available

This task uses S3KeySensor to continuously monitor the S3 bucket and verify whether the required file exists.

Purpose:

Prevent execution before file arrival
Enable automated event-driven

### Task 2 — Create Snowflake Table
create_snowflake_table

This task executes SQL queries in Snowflake to create the target table if it does not already exist.

Purpose:

Automate schema creation
Remove manual database setup

### Task 3 — Load CSV into Snowflake
tsk_copy_csv_into_snowflake_table

This task copies CSV data from Amazon S3 into Snowflake tables using Snowflake SQL commands.

Purpose:

Automate bulk data loading
Efficient ingestion of structured datasets

### End-to-End Data Flow
CSV File
   ↓
Amazon S3 Bucket
   ↓
Apache Airflow (EC2)
   ↓
S3KeySensor Validation
   ↓
Snowflake Table Creation
   ↓
COPY INTO Command
   ↓
Snowflake Data Warehouse



