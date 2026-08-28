FROM python:3.10-slim
WORKDIR /app
RUN pip install confluent-kafka requests pymongo
COPY worker.py .
CMD ["python", "worker.py"]