FROM python:3.11-alpine

# Example line to install pip packages
RUN pip install requests redis

WORKDIR /app

# Default command is to run python with the main script
CMD python /app/main.py

