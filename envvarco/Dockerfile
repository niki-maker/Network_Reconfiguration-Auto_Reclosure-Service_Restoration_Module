# Use an official Python base image
FROM python:3.9-slim

# Copy the main module (nr_module) and pyvolt into the container
COPY envvarco/envvarco /app/envvarco

# Install dependencies (if any)
RUN pip install --no-cache-dir -r /app/envvarco/requirements.txt

VOLUME ["/shared_volume_base"]

EXPOSE 4002
# Set the default command to run the script
CMD ["python", "/app/envvarco/envvarco.py"]