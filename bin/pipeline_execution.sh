base_path = "/Users/hims/Downloads/yelp_dataset/"

### Kafka Producer #####

python code/kafka/producer.py 54.193.66.237:9092 test-12 $base_path/input/yelp_academic_dataset_review.json 0 > ~/producer.log 2>&1 &

### Kafka Ingestion ####

python3 data_ingestion.py 54.193.66.237:9092 reviews s3a://yelp-data-segmentation/output/ 0.01

### raw_data_processing ###

python data_processing.py