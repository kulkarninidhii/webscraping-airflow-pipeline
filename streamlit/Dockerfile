# Use the official Python image as base
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Install Python dependencies
RUN pip install --no-cache-dir streamlit

RUN pip install python-dotenv

RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# Expose the port on which Streamlit will run (default: 8501)
EXPOSE 8501

# Command to run Streamlit when the container starts
#CMD ["streamlit", "run", "--server.port=8501","--server.address=0.0.0.0", "/app/streamlit/homepage.py"]
