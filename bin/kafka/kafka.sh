
python code/kafka/producer.py localhost:9092 test-12 /Users/hims/Downloads/yelp_dataset/yelp_academic_dataset_review.json 0 > producer.log 2>&1 &


python code/kafka/consumer.py localhost:9092 test-12 /tmp/a/