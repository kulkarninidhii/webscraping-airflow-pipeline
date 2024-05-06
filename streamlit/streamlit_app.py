import streamlit as st
import requests
import os
from dotenv import load_dotenv

fastapi_set_url = "http://uploadendapi:8001/upload"
# Load environment variables from .env file
load_dotenv()

# Use AWS credentials
aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")

# Main page title
st.title("Projects")

# Sidebar message
st.sidebar.success("Select a page above.")

# Upload file option
uploaded_files = st.file_uploader("Upload multiple files", accept_multiple_files=True)

# Display uploaded files
if uploaded_files:
    for file in uploaded_files:
        st.write("Uploaded file:", file.name)

# Button to upload to S3 via FastAPI
if st.button("Upload to S3"):
    if uploaded_files:
        try:
            # Send files to FastAPI backend for upload to S3
            for file in uploaded_files:
                files = {"file": (file.name, file.getvalue())}
                response = requests.post(fastapi_set_url, files=files)
                if response.status_code == 200:
                    st.success(f"File '{file.name}' uploaded to S3 successfully")
                else:
                    st.error(f"Failed to upload file '{file.name}' to S3. Error: {response.text}")
        except Exception as e:
            st.error(f"An error occurred: {str(e)}")
    else:
        st.warning("No files selected for upload")


