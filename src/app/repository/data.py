from pyspark import SparkFiles


class Data:

    def __init__(self, spark):
        self.spark = spark

    def read_word_count(self):
        url = "https://storage.googleapis.com/luizalabs-hiring-test/wordcount.txt"
        self.spark.sparkContext.addFile(url)

        return self.spark.sparkContext.textFile("file://" + SparkFiles.get("wordcount.txt"))

    def read_clientes_pedidos(self):
        url = "https://storage.googleapis.com/luizalabs-hiring-test/clientes_pedidos.csv"
        self.spark.sparkContext.addFile(url)

        return self.spark.read.csv("file://" + SparkFiles.get("clientes_pedidos.csv"), header=True, sep=',')
