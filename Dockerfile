# Use official Python base image
FROM python:3.13-slim

# Set working directory inside the container
WORKDIR /app

# Copy requirements first (better caching)
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the code
COPY . .

# Default command to run your ETL pipeline
CMD ["python", "main.py"]
