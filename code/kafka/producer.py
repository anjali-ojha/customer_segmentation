import datetime
import sys

from kafka import KafkaProducer
import io
import time


class Producer:

    def __init__(self, _server, _topic):
        self.topic = topic
        self.producer = KafkaProducer(bootstrap_servers=server)

    def publish(self, message):
        print(f"{datetime.datetime.now()} publishing to {self.topic = }, {message = }")
        self.producer.send(self.topic, bytes(message, encoding='utf-8'))

    def publish_review(self, data_file_path, index):
        print("Publishing review")
        with io.open(data_file_path, 'r') as f:
            for i, message in enumerate(f):
                print(i, message)
                if i <= index:
                    continue

                self.publish(message)
                if i % 10 == 0:
                    time.sleep(2)


if __name__ == "__main__":
    if len(sys.argv) >= 5:
        server = sys.argv[1]
        topic = sys.argv[2]
        file_path = sys.argv[3]
        so_far_completed = int(sys.argv[4])
        producer = Producer(server, topic)
        producer.publish_review(file_path, so_far_completed)
    else:
        print("Insufficient arguments. Usage: python producer.py <server> <topic> <file_path> <so_far_completed>")
#%%
