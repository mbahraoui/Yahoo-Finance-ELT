# Yahoo-Finance-ELT

## Overview

The Yahoo Finance ELT (Extract, Load, Transform) project focuses on extracting financial data from Yahoo Finance, loading it into AWS S3, and subsequently transforming it in AWS Redshift. The entire ELT process is automated and orchestrated using Dockerized Apache Airflow. Additionally, visualizations are created using Power BI for comprehensive data analysis.

## Project Structure

The project is organized into the following main components:

1. **Data Extraction (Yahoo Finance):**
   - Python scripts are used to extract financial data from Yahoo Finance.

2. **Data Loading (AWS S3):**
   - Extracted data is loaded into AWS S3 for storage and accessibility.
   - Utilize appropriate AWS credentials and configurations for seamless data transfer.

3. **Data Transformation (AWS Redshift):**
   - The data in AWS S3 is loaded into AWS Redshift.
   - Utilize a stored procedure in Redshift for data transformation.

4. **ELT Process Orchestration (Dockerized Airflow):**
   - Apache Airflow is Dockerized for easy deployment and orchestration of the ELT process.
   - Docker and Docker Compose files are provided to set up the Airflow environment.

5. **Data Visualization (Power BI):**
   - Power BI is used for creating visualizations and dashboards.

## Entity-Relationship Diagram

![ER Diagram](https://raw.githubusercontent.com/mbahraoui/Yahoo-Finance-ELT/main/entity%20relationship%20diagram.png)
