# Credit Card Fraud Detection System using PySpark

- [x] Add basic descriptions
- [x] Add more info about the dataset
- [ ] Add more info about the project
- [x] Add more info about the system
- [ ] Setup requirements

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
We would be using Kafka via Docker for this project.
1. Install [Docker Desktop](https://www.docker.com/products/docker-desktop)
2. Make sure you are in the root directory of the project, where you'll find the [docker-compose.yml](docker-compose.yml) file.
3. Run the command to start Kafka and Zookeeper
    ```bash
    docker-compose up -d
    ```
You do not always need to run the above command. You can start and stop Kafka and Zookeeper using the Docker Desktop GUI.