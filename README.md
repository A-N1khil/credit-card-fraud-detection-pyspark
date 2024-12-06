# Credit Card Fraud Detection System using PySpark

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

# Running the code
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
4. Run the first consumer [kafka_consumer_2](kafka_distributed_streaming/kafka_consumer_2.ipynb) notebook to start consuming messages.

# Verifying the logs
All the logs are stored in the respective folders. You can verify the logs to see the messages being produced and consumed.