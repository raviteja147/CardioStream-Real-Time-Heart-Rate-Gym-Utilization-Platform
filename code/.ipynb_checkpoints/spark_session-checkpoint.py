from pyspark.sql import SparkSession

def get_spark(app_name="SBIT"):
    spark = (
        SparkSession.builder
            .appName(app_name)
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.jars.packages",
                "io.delta:delta-core_2.12:2.3.0,"
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0")
            .master("local[*]")  # use all local cores
            .getOrCreate()
    )

    return spark