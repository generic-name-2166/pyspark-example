import json
from pyspark.sql import SparkSession


def load_data() -> dict:
    with open("src/pyspark_example/data.json") as F:
        return json.loads(F.read())


def main():
    spark = SparkSession.builder.getOrCreate()
    data = load_data()
    
    products = spark.createDataFrame(data["products"])
    categories = spark.createDataFrame(data["products"])
    links = spark.createDataFrame(data["links"])

    products.show()
    categories.show()
    links.show()


if __name__ == "__main__":
    main()
