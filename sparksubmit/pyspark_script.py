from pyspark.sql import SparkSession
import os


def drop_columns_from_dataset(input_path, output_path):
    """
    Read a CSV dataset from `input_path`, drop two columns and save the transformed dataset to `output_path`.
    """

    # Create or get a spark session
    spark = SparkSession.builder.appName("DropColumnsApp").getOrCreate()
    cwd = os.getcwd()
    print("cargando ubicacion actual")
    print(cwd)
    for entry in os.scandir('.'):
        if entry.is_file():
            print(entry.name)
    # Read the dataset from the given path
    df = spark.read.csv(input_path, header=True, inferSchema=True)
    df.show()
    # Drop the desired columns
    columns_to_drop = ["column1", "column2"]
    df_transformed = df.drop(*columns_to_drop)
    df_transformed.show()
    # Save the transformed dataset to the output path
    # df_transformed.write.csv(output_path, header=True)

    # Stop the spark session
    spark.stop()


if __name__ == "__main__":
    input_dataset_path = "./dags/repo/sparksubmit/myfile.csv"
    output_dataset_path = "./dags/repo/sparksubmit/out_myfile.csv"

    drop_columns_from_dataset(input_dataset_path, output_dataset_path)
