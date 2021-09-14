# Apache-Kafka & Spring Boot
Fundamental Concepts of Apache  Kafka Streaming Middlewear

Few samples of using Apache Kafka (Windows version) and API for Streaming Messaging


Zookeper:
zookeeper-server-start.bat ../../config/zookeeper.properties

Kafka server:
kafka-server-start.bat ../../config/server.properties

Topics create:
kafka-topics.bat --bootstrap-server localhost:9092 --create --topic <TOPIC_NAME>

Topics: src-topic,out-topic,transactions,patterns,rewards,purchases,high-points-topic,low-points-topic,joined_purchase
