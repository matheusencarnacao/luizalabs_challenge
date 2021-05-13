from service.service import Service
from pyspark.sql.functions import udf, col, collect_list, concat_ws, concat, lit

count_characters = udf(lambda x: len(x))


class Business:

    def __init__(self, spark):
        self.service = Service(spark)
        self.spark = spark

    def count_letters(self):
        df_special_characters = self.service.words_length_formated()

        df_agg = df_special_characters.groupBy("word").agg(count_characters(col("word")).alias("length"))

        df_filter = df_agg.filter(col("length") <= 10)

        maiores_que_10 = df_agg.filter(col("length") > 10).count()
        maiores_que_10_list = [["maiores_que_10", maiores_que_10]]
        df_maiores_que_10 = self.spark.createDataFrame(maiores_que_10_list, ["word", "length"])

        return df_filter.union(df_maiores_que_10)

    def clientes_bf(self):
        df_pedidos = self.service.pedidos_com_datas_formatadas()

        bf_2017 = "2017-11-24"
        bf_2018 = "2018-11-23"

        df_count_bf = df_pedidos.filter((col("data_pedido") == bf_2017) | (col("data_pedido") == bf_2018)) \
            .groupBy(col("codigo_cliente")).count() \
            .filter(col("count") > 2)

        df_lista_pedidos = df_pedidos.groupBy(col("codigo_cliente"), col("idade")) \
            .agg(concat_ws(", ",
                           collect_list(
                               concat(lit("["),
                                      concat_ws(", ", "codigo_pedido", "data_pedido"),
                                      lit("]")))).alias("lista_pedidos"))

        df_pedidos_with_count = df_lista_pedidos.alias('a')\
            .join(df_count_bf.alias('b'), df_lista_pedidos.codigo_cliente == df_count_bf.codigo_cliente, how="inner")\
            .select("a.codigo_cliente", "a.idade", "a.lista_pedidos", "b.count")\
            .filter(col("idade") < 30)

        return df_pedidos_with_count.select(
            col("codigo_cliente"),
            col("lista_pedidos"),
            col("count").alias("numero_pedidos"),
            col("idade"))
