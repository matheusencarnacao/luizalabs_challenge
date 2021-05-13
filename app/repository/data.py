import os


class Data:

    def __init__(self, spark):
        self.spark = spark

    def read_word_count(self):
        csv_path = os.getenv("INPUT_PATH")
        return self.spark.sparkContext.textFile(f"file:///{csv_path}/wordcount.txt")

    def read_clientes_pedidos(self):
        csv_path = os.getenv("INPUT_PATH")

        return self.spark.read.csv(f"file:///{csv_path}/clientes_pedidos.csv", header=True, sep=',')
