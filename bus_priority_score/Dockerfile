# Use official Python base image
FROM python:3.10-slim

# Set working directory


# Copy your app code into the container
COPY bus_priority_score/bus_priority_score /app/bus_priority_score

# Install dependencies
RUN pip install --no-cache-dir -r /app/bus_priority_score/requirements.txt

VOLUME ["/shared_volume_base"]

# Expose the port your app runs on
EXPOSE 4003

# Run the Flask app
CMD ["/bin/sh", "-c", "sleep 5 && python /app/bus_priority_score/bus_priority_score.py"]
