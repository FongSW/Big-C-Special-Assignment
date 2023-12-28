# master_project_fullstack_data
# https://github.com/rpeden/prefect-docker-compose/blob/main/docker-compose.yml#L24

version: '3'
services:
  prefect-flow:
    build:
      dockerfile: Dockerfile
    env_file:
      - .env
    volumes:
      - ./flows:/opt/prefect/flows
      - /path/on/host:/root/.prefect  # Update path as needed
      - sqlite-data:/path/to/sqlite
    command: ["python", "/opt/prefect/flows/etl_job_weekly.py", "&&", "prefect server start"] ##["python", "/opt/prefect/flows/etl_job_weekly.py", "&&", "prefect server start"]
    ports:
      - 4200:4200
    stop_grace_period: 2m  # Adjust as needed

volumes:
  prefect:
  sqlite-data:  # Define the sqlite-data volume