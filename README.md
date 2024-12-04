# Credit Card Fraud Detection System using PySpark

- [x] Add basic descriptions
- [ ] Add more info about the dataset
- [ ] Add more info about the project
- [ ] Add more info about the system
- [ ] Setup requirements

## Introduction
This project aims to develop a real-time fraud detection system leveraging Spark Streaming and machine learning for anomaly detection. The goal is to process streaming data in real-time to identify fraudulent transactions, focusing on system performance and efficiency. The system will integrate fundamental operating system concepts such as concurrency, memory management, and scheduling to handle large-scale, high-frequency data streams.

## Dataset
[Kaggle Credit Card Fraud Dataset](https://www.kaggle.com/datasets/mlg-ulb/creditcardfraud/data)

## Setup
### MacOS
1. Install Java 8 or 11
   If you have multiple versions of Java installed, you need to temporarily set JAVA_HOME to either Java 8 or 11.
   Use Homebrew to install Java 8 or 11.
   ```bash
   brew install openjdk@11
   ```
2. Install Hadoop
   ```bash
   brew install hadoop
   ```
3. Set HADOOP_HOME
    ```bash
    export HADOOP_HOME=/path/to/hadoop
   source ~/.zshrc
    ```

### Setting up Kafka
1. Download Kafka from the [official website](https://kafka.apache.org/downloads)
2. Extract the downloaded file
3. Save your Kafka directory path to the KAFKA_HOME environment variable
    ```bash
    export KAFKA_HOME=/path/to/kafka/bin
    source ~/.zshrc
    ```
4. Start Zookeeper
    ```bash
    $KAFKA_HOME/zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties
    ```
5. Start Kafka
    ```bash
    $KAFKA_HOME/kafka-server-start.sh $KAFKA_HOME/config/server.properties
    ```
6. Create a topic
    ```bash
    $KAFKA_HOME/kafka-topics.sh --create --topic transactions --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1
    ```