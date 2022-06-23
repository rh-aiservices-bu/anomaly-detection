import os
import json
from kafka import KafkaConsumer
from prediction import predict


KAFKA_BOOTSTRAP_SERVER = os.getenv('KAFKA_BOOTSTRAP_SERVER')
KAFKA_SECURITY_PROTOCOL = os.getenv('KAFKA_SECURITY_PROTOCOL', 'SASL_SSL')
KAFKA_SASL_MECHANISM = os.getenv('KAFKA_SASL_MECHANISM','PLAIN')
KAFKA_USERNAME = os.getenv('KAFKA_USERNAME')
KAFKA_PASSWORD = os.getenv('KAFKA_PASSWORD')
KAFKA_CONSUMER_GROUP = 'ml-consumer'
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC')
SAMPLE_SIZE = os.getenv('SAMPLE_SIZE')
WEBHOOK_URL = os.getenv('WEBHOOK_URL')


def main():
    # Normally, we'd never want to lose a message,
    # but we want to ignore old messages for this demo, so we set
    # enable_auto_commit=False
    # auto_offset_reset='latest' (Default)
    # This has the effect of starting from the last message.

    consumer = KafkaConsumer(KAFKA_TOPIC,
                             group_id=KAFKA_CONSUMER_GROUP,
                             bootstrap_servers=[KAFKA_BOOTSTRAP_SERVER],
                             security_protocol=KAFKA_SECURITY_PROTOCOL,
                             sasl_mechanism=KAFKA_SASL_MECHANISM,
                             sasl_plain_username=KAFKA_USERNAME,
                             sasl_plain_password=KAFKA_PASSWORD,
                             api_version_auto_timeout_ms=30000,
                             request_timeout_ms=450000)
    print(f'Subscribed to "{KAFKA_BOOTSTRAP_SERVER}" consuming topic "{KAFKA_TOPIC}"...')
    f = open("batch.csv", "a")
    f.write("timestamp,value\n")
    count = 0
    try:
        print(f'Consuming messages from topic "{KAFKA_TOPIC}"...')
        for record in consumer:
            count += 1
            msg = record.value.decode('utf-8')
            topic = record.topic
            item = json.loads(msg)
            timestamp = item["timestamp"]
            value = item["value"]
            line = timestamp + "," + value + "\n"
            f.write(line)
            print(count)
            if (count == int(SAMPLE_SIZE)):
                print("Running prediction on collected data...")
                predict(WEBHOOK_URL)
                count = 0
                f.close()
                os.remove("batch.csv")
                f = open("batch.csv", "a")
                f.write("timestamp,value\n")
                print("Continuing to collect next batch of data...")
    finally:
        print("Closing consumer...")
        consumer.close()

if __name__ == '__main__':
    main()
