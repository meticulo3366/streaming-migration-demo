FROM python:3.11-slim
RUN pip install --no-cache-dir --quiet confluent-kafka==2.6.1 fastavro==1.9.7 requests==2.32.3
WORKDIR /apps
COPY *.py /apps/
ENV PYTHONUNBUFFERED=1
