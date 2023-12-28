FROM prefecthq/prefect:0.15.4-python3.8

# Installing Supervisor
RUN apt-get update && apt-get install -y supervisor
RUN mkdir -p /var/log/supervisor

# Installing our requirements.txt file to the image and install dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt --trusted-host pypi.python.org --no-cache-dir

# Add our flow code to the image
COPY flows /opt/prefect/flows

# Set the working directory
WORKDIR /opt/prefect

# # Set prefect config set PREFECT_API_URL
# RUN prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api

# # Create a non-root user and assign necessary permissions
# RUN adduser --disabled-password --gecos "" supervisoruser \
#     && chown -R supervisoruser:supervisoruser /opt/prefect /var/log/supervisor

# Copy Supervisor configuration file
COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf

# Expose necessary ports
EXPOSE 22 80

# Start Supervisor
CMD ["/usr/bin/supervisord", "-c", "/etc/supervisor/conf.d/supervisord.conf"]
# CMD ["/usr/bin/supervisord"]