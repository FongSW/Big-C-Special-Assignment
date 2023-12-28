#!/bin/bash


# # Update API Configuration in Prefect Client: 
# prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api
# unset PREFECT_API_URL

# Additional setup if needed
echo "Hello"
python /opt/prefect/flows/etl_job_weekly.py
echo "Hello"


# Run the Prefect server
prefect server start

