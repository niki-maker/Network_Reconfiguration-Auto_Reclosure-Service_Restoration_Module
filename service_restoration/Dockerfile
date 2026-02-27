# Use an official Python base image
FROM python:3.9-slim

# Copy the main module (nr_module) and pyvolt into the container
COPY service_restoration/service_restoration /app/service_restoration

# Install dependencies (if any)
RUN pip install --no-cache-dir -r /app/service_restoration/requirements.txt

VOLUME ["/shared_volume_base"]

#For Case1 (Undervoltage condition)
CMD ["python", "/app/service_restoration/service_restoration.py"]
