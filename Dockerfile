# Use an official Python runtime as a parent image
FROM python:3.11-slim

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container at /app
COPY requirements.txt .

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application's code into the container at /app
COPY . .

# Command to run the application with increased timeouts for long-running validations
# Note: using shell-form so env vars expand
ENV WEB_CONCURRENCY=4 \
		GUNICORN_TIMEOUT=300 \
		GUNICORN_GRACEFUL_TIMEOUT=60 \
		GUNICORN_KEEP_ALIVE=5

CMD gunicorn -w ${WEB_CONCURRENCY} -k uvicorn.workers.UvicornWorker \
		--bind 0.0.0.0:8080 \
		--timeout ${GUNICORN_TIMEOUT} \
		--graceful-timeout ${GUNICORN_GRACEFUL_TIMEOUT} \
		--keep-alive ${GUNICORN_KEEP_ALIVE} \
		--log-level info \
		app.main:app
