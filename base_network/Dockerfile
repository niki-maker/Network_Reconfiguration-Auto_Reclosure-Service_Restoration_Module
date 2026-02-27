# Use an official Python base image
FROM python:3.9-slim

# Copy the main module (nr_module) and pyvolt into the container
COPY base_network/base_network /app/base_network

# Install dependencies (if any)
RUN pip install --no-cache-dir -r /app/base_network/requirements.txt

VOLUME ["/shared_volume_base"]

#For Case1 (Undervoltage condition)
CMD ["python", "/app/base_network/base_network.py"]
