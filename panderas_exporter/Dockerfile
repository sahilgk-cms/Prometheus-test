FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Expose the exporter port
EXPOSE 8000

# Run the exporter script
CMD ["python", "panderas_test.py"]

