# Use an official Python base image
FROM python:3.9-slim

COPY ntp /app/ntp
RUN pip install --no-cache-dir -r /app/ntp/requirements.txt

CMD ["python", "/app/ntp/NTP.py"]