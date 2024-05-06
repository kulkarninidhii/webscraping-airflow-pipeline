from fastapi import FastAPI, HTTPException
from dotenv import load_dotenv
import os
import snowflake.connector

# Load environment variables from .env file
load_dotenv()

# Initialize FastAPI app
app = FastAPI()

# Function to establish Snowflake connection and fetch data
def fetch_data_from_snowflake():
    try:
        # Retrieve Snowflake credentials from environment variables
        SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
        SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
        SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
        SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
        SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
        SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")

        # Establish Snowflake connection
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA
        )

        # Execute query to fetch data
        query = f"SELECT * FROM {os.getenv('SNOWFLAKE_TABLE')}"
        with conn.cursor() as cur:
            cur.execute(query)
            # Fetch all rows
            rows = cur.fetchall()
        return rows
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/get_data")
async def get_data():
    try:
        # Fetch data from Snowflake
        data = fetch_data_from_snowflake()
        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
