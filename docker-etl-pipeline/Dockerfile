FROM python:3.12-slim

WORKDIR /app

RUN pip install pandas pyarrow boto3 requests

COPY etl.py .

CMD ["python3", "etl.py"]
