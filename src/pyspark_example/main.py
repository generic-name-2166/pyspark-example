import json
from pyspark.sql import SparkSession, DataFrame


def load_data() -> dict:
    with open("src/pyspark_example/data.json") as F:
        return json.loads(F.read())


def run(
    products: DataFrame,
    categories: DataFrame,
    links: DataFrame,
) -> DataFrame:
    join = links.join(products, on="product_id", how="fullouter").withColumnRenamed(
        "name", "product_name"
    )

    return (
        join.join(categories, on="category_id", how="left")
        .withColumnRenamed("name", "category_name")
        .select("product_name", "category_name")
    )


def main():
    spark = SparkSession.builder.getOrCreate()
    data = load_data()

    products = spark.createDataFrame(data["products"])
    categories = spark.createDataFrame(data["categories"])
    links = spark.createDataFrame(data["links"])
    # products.show()
    # categories.show()
    # links.show()
    result = run(products, categories, links)
    result.show()


if __name__ == "__main__":
    main()
