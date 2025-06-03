**YouTube Live Stream Data Pipeline**

This project automates the extraction, processing, and loading of YouTube live stream data into Amazon Redshift using a scalable, cloud-native architecture built on AWS and Databricks. The pipeline ensures timely data ingestion, transformation, and analytics readiness for livestream metadata.

**Tech Stack**

- **AWS Services**
  - S3 (for storing Parquet and control files)
  - Lambda (executes Redshift COPY commands)
  - SNS (triggers Lambda and sends email notifications)
  - Redshift (data warehouse)
  - IAM, VPC (for security and network configuration)
- **Databricks on AWS**
  - Executes scheduled jobs to extract and write data
- **YouTube Data API v3**
  - Retrieves live stream metadata
- **Parquet Format**
  - Used for efficient, columnar data storage

---

## Project Overview

This pipeline follows an end-to-end data workflow for ingesting and loading YouTube livestream metadata:

1. **Data Ingestion**: A Databricks notebook queries the YouTube Data API for live stream details and writes the data to Amazon S3 in Parquet format.
2. **Manifest Generation**: A manifest file is created to list all output Parquet files.
3. **Control File Creation**: A `.txt` file containing a Redshift `COPY` command is written to a separate S3 location.
4. **Trigger Mechanism**: Upon the control file's arrival in S3, an SNS topic is triggered.
5. **Data Loading**: The SNS topic invokes an AWS Lambda function, which runs the Redshift `COPY` command to load the Parquet data into a Redshift table.
6. **Notification**: After successful data loading, SNS sends an email notification to the stakeholders.

---
## Architecture Diagram 



## How to Run

### Prerequisites

- AWS account with appropriate permissions
- S3 buckets created for Parquet and control files
- Redshift cluster and table created
- YouTube API key
- Databricks workspace set up on AWS

### Execution Steps

1. **Upload Databricks Notebook**  
   Upload `youtube_ingest_notebook.py` to your Databricks workspace and configure it as a scheduled job.

2. **Create Redshift COPY Command**  
   Write the COPY command into `copy_command.txt` using the manifest as input.

3. **Configure Lambda and SNS**  
   Deploy `lambda_function.py`, configure its execution role, and link it with an SNS topic.

4. **S3 and SNS Integration**  
   Use S3 Event Notifications to publish to SNS when a `.txt` file lands in the control folder.

5. **Email Notification**  
   Subscribe your email to the SNS topic to receive pipeline completion alerts.

---



