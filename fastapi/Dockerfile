# Use the official Python image as base
FROM python:3.9-slim

# Set the working directory in the container

# Install Python dependencies
RUN pip install --no-cache-dir fastapi uvicorn python-dotenv snowflake-connector-python boto3
RUN pip install python-multipart

#RUN pip install -r /Users/nidhikulkarni/fastapi/requirements.txt

# Expose the port on which FastAPI will run (default: 8000)

COPY . /app
WORKDIR /app
# Command to run FastAPI when the container starts
#CMD ["uvicorn", "Displayapi:app", "--host", "0.0.0.0", "--port", "8000"]
EXPOSE 8000
EXPOSE 8001
