# webscraping-airflow-pipeline
An airflow pipeline to perform webscraping, text extraction, cleaning and storage

## Problem Statement
The aim is to automate PDF data extraction and storage into Snowflake using Airflow and FastAPI. Streamlit serves as the user interface for PDF upload and triggering the pipeline. Dockerize services and host online for seamless integration.

## Project Goals
The task involves building an end-to-end pipeline utilizing Airflow for automating the extraction and storage of meta-data and content from PDF files into Snowflake. This requires the development of two API services using FastAPI. The first API service will trigger an Airflow pipeline upon receiving the S3-file location, facilitating extraction, data validation, and loading of data and metadata into Snowflake. The second API service will interact with Snowflake to execute queries and return responses. Additionally, the Streamlit framework will serve as the interface for the end-user application to upload PDF files, initiate the pipeline, and display the results obtained from Snowflake.

## Technologies Used
- Airflow
- FastAPI
- Snowflake
- Streamlit
- Docker
- AWS S3

## Run Project Locally

1. To run the project clone this repo and update the .env with appropriate credentials.

2. Run the docker commands to bring up the Airflow instance, FastAPI, Streamlit.

   ```
   docker compose up -d
   ```

