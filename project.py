import os

from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, lit, struct, unix_timestamp, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, LongType

# метод для записи данных в 2 target: в PostgreSQL для фидбэков и в Kafka для триггеров
def foreach_batch_function(df, epoch_id):
    # сохраняем df в памяти, чтобы не создавать df заново перед отправкой в Kafka
    df.cache() # тут поначала хотел использовать что-то типа df.write или df.writeStream (не знаю, почему)
    # записываем df в PostgreSQL с полем feedback
    df = df.withColumn('feedback', lit(None))
    # создаём df для отправки в Kafka. Сериализация в json.
    df.write \
        .mode('append') \
        .format('jdbc') \
        .option('url', 'jdbc:postgresql://rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de') \
        .option('driver', 'org.postgresql.Driver') \
        .option('dbtable', 'subscribers_feedback') \
        .option('user', 'student') \
        .option('password', 'de-student') \
        .save() # тут опять путал write и writeStream (думал, если рабоаем через Кафку, значит и писать надо в формате стриминга)
    # отправляем сообщения в результирующий топик Kafka без поля feedback
    df = df.drop("feedback")
    
    kafka_df = df.select(to_json(struct(*df.columns)).alias("value")) # процесс сборки json из строки не усвоил (даже принцип своими словами объяснить затрудняюсь)

    kafka_df.write \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091') \
            .option('topic', 's25321830_out') \
            .save() # запутался из-за того, что приходится 2 раза использовать .write
    # очищаем память от df
    df.unpersist() # эта функция забылась, вылетела из головы
    

# необходимые библиотеки для интеграции Spark с Kafka и PostgreSQL
spark_jars_packages = ",".join(
        [
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
            "org.postgresql:postgresql:42.4.0",
        ]
    )

# создаём spark сессию с необходимыми библиотеками в spark_jars_packages для интеграции с Kafka и PostgreSQL
spark = SparkSession.builder \
    .appName("RestaurantSubscribeStreamingService") \
    .config("spark.sql.session.timeZone", "UTC") \
    .config("spark.jars.packages", spark_jars_packages) \
    .getOrCreate()

# читаем из топика Kafka сообщения с акциями от ресторанов 
restaurant_read_stream_df = spark.readStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091') \
    .option('kafka.security.protocol', 'SASL_SSL') \
    .option('kafka.sasl.jaas.config', 'org.apache.kafka.common.security.scram.ScramLoginModule required username="login" password="password";') \
    .option('kafka.sasl.mechanism', 'SCRAM-SHA-512') \
    .option('subscribe', 's25321830_in') \
    .load()

# определяем схему входного сообщения для json
incomming_message_schema = StructType([
    StructField("restaurant_id", StringType()),
    StructField("adv_campaign_id", StringType()),
    StructField("adv_campaign_content", StringType()),
    StructField("adv_campaign_owner", StringType()),
    StructField("adv_campaign_owner_contact", StringType()),
    StructField("adv_campaign_datetime_start", LongType()),
    StructField("adv_campaign_datetime_end", LongType()),
    StructField("datetime_created", LongType())
]) # ну тут понятно, всё по примеру сообщения из условий задания

# определяем текущее время в UTC в миллисекундах, затем округляем до секунд
current_timestamp_utc = int(round(unix_timestamp(current_timestamp())))

# десериализуем из value сообщения json и фильтруем по времени старта и окончания акции
filtered_read_stream_df = restaurant_read_stream_df \
    .select(
        from_json(col("value").cast("string"), incomming_message_schema).alias("parsed")
    ) \
    .select("parsed.*") # опять трудности с сериализацией/десериализацией

# вычитываем всех пользователей с подпиской на рестораны
subscribers_restaurant_df = spark.read \
                    .format('jdbc') \
                    .option('url', 'jdbc:postgresql://rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de') \
                    .option('driver', 'org.postgresql.Driver') \
                    .option('dbtable', 'subscribers_restaurants') \
                    .option('user', 'student') \
                    .option('password', 'de-student') \
                    .load()

# джойним данные из сообщения Kafka с пользователями подписки по restaurant_id (uuid). Добавляем время создания события.
result_df = filtered_read_stream_df.join(subscribers_restaurant_df, on=['restaurant_id'], how='inner') \
    .withColumn('trigger_datetime_created', current_timestamp_utc) # тут вроде несложно
# запускаем стриминг
result_df.writeStream \
    .foreachBatch(foreach_batch_function) \
    .start() \
    .awaitTermination()