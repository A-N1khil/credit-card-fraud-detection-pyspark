# Credit Card Fraud Detection System using PySpark

## Contents
1. [Introduction](#introduction)
2. [Dataset](#dataset)
3. [Setup](#setup)
    1. [Setting up Hadoop](#setting-up-hadoop)
    2. [Setting up your Python env](#setting-up-your-python-env)
    3. [Setting up Scala](#setting-up-scala)
    4. [Setting up Kafka](#setting-up-kafka)
4. [Running the code](#running-the-code)
    1. [Running the one with single producer and consumer](#running-the-one-with-single-producer-and-consumer)
    2. [Running the one with multiple producers and consumers](#running-the-one-with-multiple-producers-and-consumers)
    3. [Verifying the logs](#verifying-the-logs)
5. [Code files and their description](#code-files-and-their-description)

## Introduction
This project aims to develop a real-time fraud detection system leveraging Spark Streaming and machine learning for anomaly detection. The goal is to process streaming data in real-time to identify fraudulent transactions, focusing on system performance and efficiency. The system will integrate fundamental operating system concepts such as concurrency, memory management, and scheduling to handle large-scale, high-frequency data streams.

## Dataset
This dataset needs to be installed and the csv needs to be placed in the dataset folder. Please check reference for example  
[Kaggle Credit Card Fraud Dataset](https://www.kaggle.com/datasets/mlg-ulb/creditcardfraud/data)

## Setup

> [!NOTE]  
> This project has been developed on macOS. The setup instructions are for macOS. You can adapt them to your operating system.
> Most of these commands need to be run in the root directory of the project, unless stated otherwise.

### Setting up Hadoop

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

### Setting up your Python env
1. Install requirements
    ```bash
    pip install -r requirements.txt
    ```

### Setting up Scala
> [!WARNING]
> You need to have Scala installed on your system to run the Scala code.  
> This project uses Scala 2.12. You can install it using Homebrew.
 ```bash
 brew install scala@2.12
 ```

Verify your installation by running the following command:
```bash 
scala -version
```

### Setting up Kafka
1. Download Kafka from the [official website](https://kafka.apache.org/downloads).  
   or via wget
   ```bash
    wget https://downloads.apache.org/kafka/2.8.0/kafka_2.13-2.8.0.tgz
    ```
2. Extract the installation files and note the directory as `$KAFKA`. Do not set the bin directory in your PATH
3. Kafka requires Zookeeper to run. Start Zookeeper.
    ```bash
    $KAFKA/bin/zookeeper-server-start.sh $KAFKA/config/zookeeper.properties
    ```
4. Start Kafka
    ```bash
    $KAFKA/bin/kafka-server-start.sh $KAFKA/config/server.properties
    ```

## Running the code
## Running the one with single producer and consumer
1. Create a topic
    ```bash
    $KAFKA/bin/kafka-topics.sh --create --topic transactions --bootstrap-server localhost:9092
    ```
2. Run the producer [kafka_producer](kafka_streaming/kafka_producer.ipynb) notebook to start producing messages.
3. Run the consumer [kafka_consumer](kafka_streaming/kafka_consumer.ipynb) notebook to start consuming messages.

## Running the one with multiple producers and consumers
1. Create a topic
    ```bash
    $KAFKA/bin/kafka-topics.sh --create --topic distributed_transactions --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
    ```
2. Run the producer [kafka_distributed_producer](kafka_distributed_streaming/kafka_distributed_producer.ipynb) notebook to start producing messages.
3. Run the first consumer [kafka_consumer_1](kafka_distributed_streaming/kafka_consumer_1.ipynb) notebook to start consuming messages.
4. Run the second consumer [kafka_consumer_2](kafka_distributed_streaming/kafka_consumer_2.ipynb) notebook to start consuming messages.

### Verifying the logs
All the logs are stored in the respective folders. You can verify the logs to see the messages being produced and consumed.

## Code files and their description
1. [kafka_producer](kafka_streaming/kafka_producer.ipynb) - This notebook contains the code for the producer that produces messages to the Kafka topic.
2. [kafka_consumer](kafka_streaming/kafka_consumer.ipynb) - This notebook contains the code for the consumer that consumes messages from the Kafka topic.
3. [kafka_distributed_producer](kafka_distributed_streaming/kafka_distributed_producer.ipynb) - This notebook contains the code for the distributed producer that produces messages to the Kafka topic.
4. [kafka_consumer_1](kafka_distributed_streaming/kafka_consumer_1.ipynb) - This notebook contains the code for the first consumer that consumes messages from the Kafka topic.
5. [kafka_consumer_2](kafka_distributed_streaming/kafka_consumer_2.ipynb) - This notebook contains the code for the second consumer that consumes messages from the Kafka topic.
6. [credit_card_fraud_detection](credit-card-fraud-detection.ipynb) - This notebook contains the code for the credit card fraud detection system using PySpark.
7. [pipelining](pipelining.ipynb) - This notebook contains the code for pipelining in PySpark.