import findspark
from pyspark.sql import SparkSession
import os

from business.business import Business

findspark.init()


def count_letters(business, output_path):
    df = business.count_letters()
    df.repartition(1).write\
        .option("header", "true")\
        .option("sep", ",")\
        .mode("overwrite")\
        .csv(f"{output_path}/count_letters.csv")
    

def clients_bf(business, output_path):
    df = business.clientes_bf()
    df.repartition(1).write\
        .option("header", "true")\
        .option("sep", ",")\
        .mode("overwrite")\
        .csv(f"{output_path}/clients_orders_bf.csv")


def main():
    spark = SparkSession.\
        builder.\
        appName("LuizaLabs"). \
        master("spark://spark-master:7077"). \
        config("spark.executor.memory", "256m"). \
        getOrCreate()

    output_csv = os.getenv("OUTPUT_CSV")

    business = Business(spark)
    count_letters(business, output_csv)
    clients_bf(business, output_csv)


if __name__ == "__main__":
    main()
