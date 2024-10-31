# Dockerfile.python
FROM python:3.10-slim

# Copy your Python application and setup.py into the container
COPY setup.py setup.py


# Install any necessary dependencies (e.g., pika for RabbitMQ communication)
RUN pip install pika

# Run your setup.py script when the container starts
CMD ["python3", "setup.py"]