# Use an official Python base image
FROM python:3.9-slim

# Copy the main module (nr_module) and pyvolt into the container
COPY auto_reclosure/auto_reclosure /app/auto_reclosure

# Install dependencies (if any)
RUN pip install --no-cache-dir -r /app/auto_reclosure/requirements.txt

VOLUME ["/shared_volume_base"]

#For Case1 (Undervoltage condition)
CMD ["python", "/app/auto_reclosure/auto_reclosure.py"]