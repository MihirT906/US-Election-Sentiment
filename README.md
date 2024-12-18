# US-Election-Sentiment
Real-Time Sentiment Analysis of US Election Tweets/Posts

- Using Python 3.10.1
- Other prerequisites in 

### Environment Setup
1. Download kafka from https://kafka.apache.org/downloads
2. Start Zookeeper: In the Kafka directory, run the following command to start Zookeeper.
```
bin/zookeeper-server-start.sh config/zookeeper.properties
```
typically runs on port 2181 \n
3. Start Kafka server: In a new terminal window, start Kafka using the following command
```
bin/kafka-server-start.sh config/server.properties
```
Once Kafka is running, the broker will be listening on localhost:9092 \n
4. Verify Kafka Broker Status:
```
bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```
If Kafka is running, this command will either display a list of existing topics (or nothing if you haven’t created any topics yet) \n
5. Create topic:
```
bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 12 --topic reddit_comments
```
Find your topic using:
```
bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```
6. Run Producer: \n
Add credentials to a file credentials.cfg of the format:
```
[DEFAULT]
CLIENT_ID = 
SECRET_KEY = 
USERNAME = 
PASSWORD = 
USER_AGENT = 
```
And then run:
```
python reddit_producer.py
```
7. Run Consumer
```
python reddit_consumer.py
```
8. View it on Spark
```
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 stream_processor.py
```
