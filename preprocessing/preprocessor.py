from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.sql import DataFrame

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