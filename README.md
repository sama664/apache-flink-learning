# apache-flink-learning

Docker ps 

docker compose up -d

[//]: # (Docekr commands to create Kafka topics for testing)
docker exec -ti kafka kafka-topics --create --topic=input-topic --partitions 3 --if-not-exists --bootstrap-server=kafka:9093
docker exec -ti kafka kafka-topics --create --topic=output-topic --partitions 3 --if-not-exists --bootstrap-server=kafka:9093
docker exec -ti kafka kafka-topics --create --topic=source2-topic --partitions 3 --if-not-exists --bootstrap-server=kafka:9093
docker exec -ti kafka kafka-topics --list --bootstrap-server=kafka:9093

[//]: # (producer commands to send messages to Kafka topics for testing)
docker exec -ti kafka kafka-console-producer --topic input-topic --bootstrap-server kafka:9093
docker exec -ti kafka kafka-console-producer --topic source2-topic --bootstrap-server kafka:9093