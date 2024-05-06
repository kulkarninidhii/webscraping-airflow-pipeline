import streamlit as st
import requests
from requests.auth import HTTPBasicAuth
from datetime import datetime

# Replace with your actual Airflow username and password
airflow_username = "airflow"
airflow_password = "airflow"

# The full URL to trigger the DAG
trigger_dag_url = "http://airflow-webserver:8080/api/v1/dags/consolidated_dag/dagRuns"

dag_run_id = "manual__" + datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

# Optional: Payload for the POST request, e.g., to pass configuration to the DAG run
#payload = {
#    "conf": {
#        "key": "value"  # Example configuration, adjust as needed
#    }
#}

# Make the POST request to trigger the DAG

# Proceed with handling the response...


payload_web = {
    "dag_run_id": dag_run_id,
    "conf": { "trigger_source":"button1"}  # Include any necessary DAG configuration here
}

payload_ex = {
    "dag_run_id": dag_run_id,
    "conf": { "trigger_source":"button2"}  # Include any necessary DAG configuration here
}

fastapi_get_url = "http://displayapi:8000/get_data"

# Set page configuration
st.set_page_config(
    page_title="Projects",
)

# Main page title
st.title("Projects")

# Sidebar message
st.sidebar.success("Select a page above.")

# Retrieve my_input from session state with default value
my_input = st.session_state.get("my_input", "")


# If user clicks on "Extract" button
if st.button("Scrape Data"):
    # Code to trigger Airflow for data extraction
    response = requests.post(
    trigger_dag_url,
    json=payload_web,  # Include payload if necessary
    auth=HTTPBasicAuth(airflow_username, airflow_password)  # Basic Auth
)
    st.write("Scraping initiated!")
    #Write code to trigger airflow rest api

if st.button("Extract Data"):
    response = requests.post(
    trigger_dag_url,
    json=payload_ex,  # Include payload if necessary
    auth=HTTPBasicAuth(airflow_username, airflow_password)  # Basic Auth
)
    # Code to trigger Airflow for data extraction
    st.write("Data extraction initiated!")
    #Write code to trigger airflow rest api

# If user clicks on "Display" button
if st.button("Display"):
    # Send request to FastAPI to fetch data from Snowflake
    response = requests.get(fastapi_get_url)
    if response.status_code == 200:
        data = response.json()
        st.write("Displaying results...")
        # Display data in a table format
        st.table(data)
    else:
        st.error("Failed to fetch data from Snowflake.")
