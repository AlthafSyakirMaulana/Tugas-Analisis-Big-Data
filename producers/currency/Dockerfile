FROM python:3.11-slim
WORKDIR /app
RUN pip install requests confluent-kafka
COPY app.py .
CMD ["python", "app.py"]
