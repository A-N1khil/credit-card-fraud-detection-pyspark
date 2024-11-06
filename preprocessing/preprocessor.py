from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.sql import DataFrame
from pyspark.sql.functions import col

def cleaner(df: DataFrame) -> DataFrame:
    """
    This function will clean the dataset by removing the rows with missing values.
    :param df: The input dataframe
    :return: The cleaned dataframe
    """

    original_size = df.count()
    # Clean up missing values
    df_cleaned = df.na.drop()
    cleaned_size = df_cleaned.count()
    print(f'Removed {original_size - cleaned_size} rows')
    return df_cleaned

def assemble_features(df: DataFrame) -> DataFrame:
    """
    This function will assemble the cleaned data into a single column
    :param df: The input dataframe
    :return: The assembled dataframe
    """

    # Select feature columns (all except 'Class', which is the label)
    feature_columns = df.columns[:-1]  # Assuming the last column is the label

    # Assemble feature columns into a single vector column called "features"
    assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
    df_transformed = assembler.transform(df)

    # Return the transformed data
    return df_transformed

def scale_features(df: DataFrame) -> DataFrame:
    """
    This function will scale the features in the dataframe
    :param df: The input dataframe
    :return: The scaled dataframe
    """

    # Scale features
    scaler = StandardScaler(inputCol="features", outputCol="scaled_features", withStd=True, withMean=False)
    df_scaled = scaler.fit(df).transform(df)

    # Summary of the scaling process
    print(f"Mean: {scaler.explainParams()}")
    # print(f"Standard Deviation: {scaler.std}")

    # Return the scaled data
    return df_scaled

def class_imbalance(df: DataFrame) -> DataFrame:
    """
    This function will handle class imbalance in the dataset
    :param df: The input dataframe
    :return: The balanced dataframe
    """

    # Count the number of instances in each class
    # Count the number of fraudulent and non-fraudulent transactions
    fraud_count = df.filter(col('Class') == 1).count()
    non_fraud_count = df.filter(col('Class') == 0).count()
    print(f"Fraudulent transactions: {fraud_count}\nNon-fraudulent transactions: {non_fraud_count} before balancing")

    # Undersampling the non-fraudulent transactions
    df_fraud = df.filter(col('Class') == 1)
    df_non_fraud = df.filter(col('Class') == 0).sample(fraction=fraud_count / non_fraud_count)

    # Combine the two classes
    df_balanced = df_fraud.union(df_non_fraud)

    return df_balanced

def split_data(df: DataFrame, seed = 42) -> (DataFrame, DataFrame):
    """
    This function will split the data into training and testing sets
    :param seed: Seed for reproducibility
    :param df: The input dataframe
    :return: The training and testing dataframes
    """

    # Split the data into training and testing sets
    return df.randomSplit([0.8, 0.2], seed)