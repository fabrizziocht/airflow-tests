from pyspark.sql import SparkSession


def drop_columns_from_dataset(input_path, output_path):
    """
    Read a CSV dataset from `input_path`, drop two columns and save the transformed dataset to `output_path`.
    """

    # Create or get a spark session
    spark = SparkSession.builder.appName("DropColumnsApp").getOrCreate()

    # Read the dataset from the given path
    df = spark.read.csv(input_path, header=True, inferSchema=True)

    # Drop the desired columns
    columns_to_drop = ["column1", "column2"]
    df_transformed = df.drop(*columns_to_drop)

    # Save the transformed dataset to the output path
    df_transformed.write.csv(output_path, header=True)

    # Stop the spark session
    spark.stop()


if __name__ == "__main__":
    input_dataset_path = "/test_data/myfile.csv"
    output_dataset_path = "/test_data/myfile_out.csv"

    drop_columns_from_dataset(input_dataset_path, output_dataset_path)
