import confluent_kafka
import requests
import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# Kafka consumer configuration
kafka_conf = {
    'bootstrap.servers': 'KAFKA_FQDN_OR_IP:KAFKA_PORT',
    'group.id': 'GROUP_ID',
    'auto.offset.reset': 'earliest'
}

# Splunk HEC configuration
splunk_url = 'https://SPLUNK_HEC_URL:8088/services/collector/raw'
splunk_token = 'SPLUNK_HEC_TOKEN'
headers = {
'Authorization': f'Splunk {splunk_token}',
'Content-Type': 'application/json'
}

# Initialize Kafka consumer
consumer = confluent_kafka.Consumer(kafka_conf)
consumer.subscribe(['KAFKA_TOPIC'])

# Initialize HTTP session
session = requests.Session()
session.headers.update(headers)

# Function to send log to Splunk
def send_to_splunk(log_message):
    max_retries = 3
    for attempt in range(max_retries):
        try:
            response = session.post(splunk_url, data=json.dumps({"event": log_message, "sourcetype": "_json"}))
            response.raise_for_status()
            logging.info(f'Sent message to Splunk: {log_message}')
            logging.info(f'Response: {response.text}')
            return True
        except requests.RequestException as e:
            logging.error(f'Failed to send event to Splunk (attempt {attempt + 1}/{max_retries}): {e}')
            time.sleep(2)
            return False

# Function to consume messages from Kafka and process them
def consume_and_process():
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            logging.error(f'Consumer error: {msg.error()}')
            continue
        log_message = msg.value().decode('utf-8')
        logging.info(f'Received message: {log_message}')
        if send_to_splunk(log_message):
            consumer.commit(msg)

# Main function to run the consumer with parallel processing
def main():
    logging.basicConfig(level=logging.INFO)
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(consume_and_process) for _ in range(10)]
    for future in as_completed(futures):
        try:
            future.result()
        except Exception as e:
            logging.error(f'Error in consumer thread: {e}')


if __name__ == '__main__':
    main()
