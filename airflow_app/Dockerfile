# Use an official Airflow image as the base
FROM apache/airflow:2.8.3

# Install system dependencies required by Playwright
USER root
RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y wget fonts-liberation libasound2 libatk-bridge2.0-0 libatk1.0-0 libcups2 libdbus-1-3 libgdk-pixbuf2.0-0 libnspr4 libnss3 libxcomposite1 libxdamage1 libxkbcommon0 libxrandr2 xdg-utils libu2f-udev libvulkan1 libgbm1 python3-pip python3-dev libxcb-dri3-0 gtk+3.0 \
    libdbus-glib-1-2 libxt6 libx11-xcb1 libxcomposite1 libxdamage1 libxrandr2 libasound2 libpango-1.0-0 libpangocairo-1.0-0 libfreetype6 libfontconfig1 libxcb-shm0 libxcb-render0 libxcb-shape0 libcairo2 libcairo-gobject2 libgdk-pixbuf2.0-0 libxi6 libxrender1 libxslt1.1 libssl3 && \
    rm -rf /var/lib/apt/lists/*



# Switch back to the Airflow user
USER airflow

# Use the Airflow user to install Python packages
RUN pip install --no-cache-dir scrapy scrapy-playwright playwright pydantic \
    apache-airflow-providers-amazon apache-airflow-providers-snowflake dbt-core dbt-snowflake PyPDF2 pandas boto3

RUN pip install --upgrade dbt-snowflake

# Run Playwright install to ensure all necessary browser binaries are installed
RUN playwright install

# Continue with any other setup you may need
EXPOSE 9099
