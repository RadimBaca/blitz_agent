# Base image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Copy requirements first to leverage Docker cache
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy app files
COPY . .

# Expose port
EXPOSE 5000

# Set immediate output for python logs (no buffering)
ENV PYTHONUNBUFFERED=1

# Entry point
CMD ["python", "app.py"]
