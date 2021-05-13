from repository.data import Data
from pyspark.sql.functions import lower, col, udf
from pyspark.sql.types import StringType
import re
from datetime import date, datetime


def calculate_age(born):
    today = date.today()
    return today.year - born.year - ((today.month, today.day) < (born.month, born.day))


remove_special_characters = udf(lambda x: re.sub("[^0-9a-zA-Z$]+", "", x), StringType())
convert_age = udf(lambda x: calculate_age(datetime.fromisoformat(x).date()))
convert_timestamp = udf(lambda x: str(date.fromtimestamp(int(x))))


class Service:

    def __init__(self, spark):
        self.data = Data(spark)

    def words_length_formated(self):
        txt_rdd = self.data.read_word_count()

        wc = txt_rdd.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 0))

        df_wc = wc.toDF(["word", "length"])

        df_lower_letters = df_wc.select(lower(col('word')).alias('word'), col("length"))

        return df_lower_letters.select(remove_special_characters(col("word")).alias("word"), col("length"))

    def pedidos_com_datas_formatadas(self):
        df_pedidos = self.data.read_clientes_pedidos()

        return df_pedidos.select(col("codigo_pedido"),
                                 col("codigo_cliente"),
                                 convert_age(col("data_nascimento_cliente")).alias("idade"),
                                 convert_timestamp(col("data_pedido")).alias("data_pedido"))
