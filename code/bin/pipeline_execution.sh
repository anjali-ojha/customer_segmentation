base_path = "/Users/hims/Downloads/yelp_dataset/"

### Kafka Producer #####

python code/kafka/producer.py localhost:9092 test-12 $base_path/input/yelp_academic_dataset_review.json 0 > ~/producer.log 2>&1 &

### Kafka Ingestion ####

python code/kafka/consumer.py localhost:9092 test-12 $base_path/output/

### raw_data_processing ###

python data_processing.py