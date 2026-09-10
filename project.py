import os
import config
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, lit, struct, unix_timestamp, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, LongType
from pyspark.sql.streaming import Trigger

def foreach_batch_function(df, epoch_id): 
    # Решил во второй версии проекта не разбивать на foreach_batch_function_only_postgres и foreach_batch_function_only_kafka, иначе я сам запутаюсь.
    df.cache()
    df = df.withColumn('feedback', lit(None))
    df.write \
        .mode('append') \
        .format('jdbc') \
        .option('url', config.PG_WRITE_URL) \
        .option('driver', config.PG_WRITE_DRIVER) \
        .option('dbtable', config.PG_WRITE_TABLE) \
        .option('user', config.PG_WRITE_USER) \
        .option('password', config.PG_WRITE_PASSWORD) \
        .save()
    df = df.drop("feedback")
    kafka_df = df.select(to_json(struct(*df.columns)).alias("value"))
    kafka_df.write \
            .format('kafka') \
            .option('kafka.bootstrap.servers', config.KAFKA_BOOTSTRAP_SERVERS) \
            .option('topic', config.KAFKA_TOPIC_OUT) \
            .save()
    df.unpersist()


spark_jars_packages = ",".join(
        [
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
            "org.postgresql:postgresql:42.4.0",
        ]
    )

spark = SparkSession.builder \
    .appName("RestaurantSubscribeStreamingService") \
    .config("spark.sql.session.timeZone", "UTC") \
    .config("spark.jars.packages", spark_jars_packages) \
    .getOrCreate()

def reading_from_kafka ():
    # Сделал функцию, параметры импортирую через отдельный файл, буду ссылаться на него.
    # Не знаю, опасно ли сохранять файл с паролями (config.py) в той же папке, что и скрипт, если тоже становится фактически общедоступным, 
    # но для учебных целей, я думаю, не смертельно.
    restaurant_read_stream_df = spark.readStream \
        .format('kafka') \
        .option('kafka.bootstrap.servers', config.KAFKA_BOOTSTRAP_SERVERS) \
        .option('kafka.security.protocol', config.KAFKA_SECURITY_PROTOCOL) \
        .option('kafka.sasl.jaas.config', config.KAFKA_JAAS_CONFIG) \
        .option('kafka.sasl.mechanism', config.KAFKA_SASL_MECHANISM) \
        .option('subscribe', config.KAFKA_TOPIC_IN) \
        .load()
    return restaurant_read_stream_df

restaurant_read_stream_df = reading_from_kafka() # Добавил вызов функции, чтобы ссылаться на полученный датафрейм

incomming_message_schema = StructType([
    StructField("restaurant_id", StringType()),
    StructField("adv_campaign_id", StringType()),
    StructField("adv_campaign_content", StringType()),
    StructField("adv_campaign_owner", StringType()),
    StructField("adv_campaign_owner_contact", StringType()),
    StructField("adv_campaign_datetime_start", LongType()),
    StructField("adv_campaign_datetime_end", LongType()),
    StructField("datetime_created", LongType())
])

def filter_active_campaigns(df): # Вынес парсинг JSON и фильтр по времени в отдельную функцию
    filtered_read_stream_df = df \
        .select(
            from_json(col("value").cast("string"), incomming_message_schema).alias("parsed")
        ) \
        .select("parsed.*") \
        .where(
            (col("adv_campaign_datetime_start") <= unix_timestamp(current_timestamp())) &
            (col("adv_campaign_datetime_end") >= unix_timestamp(current_timestamp()))
        )
    
    return filtered_read_stream_df

def reading_subscribers_from_postgres():
    # Сделал чтение подписчиков отдельной функцией, тут тоже подтягиваю параметры из нового файла
    subscribers_restaurant_df = spark.read \
                        .format('jdbc') \
                        .option('url', config.PG_READ_URL) \
                        .option('driver', config.PG_READ_DRIVER) \
                        .option('dbtable', config.PG_READ_TABLE) \
                        .option('user', config.PG_READ_USER) \
                        .option('password', config.PG_READ_PASSWORD) \
                        .load()
    return subscribers_restaurant_df

subscribers_restaurant_df = reading_subscribers_from_postgres() # Вызываю функцию

filtered_read_stream_df = filter_active_campaigns(restaurant_read_stream_df) # Вызываю функцию парсинга и фильтра
result_df = filtered_read_stream_df.join(subscribers_restaurant_df, on=['restaurant_id'], how='inner') \
    .withColumn('trigger_datetime_created', unix_timestamp(current_timestamp()))

result_df.writeStream \
    .foreachBatch(foreach_batch_function) \
    # Добавил чекпоинт
    .option('checkpointLocation', config.CHECKPOINT_PATH) \
    # Добавил триггер
    .trigger(Trigger.ProcessingTime('5 minutes')) \
    .start() \
    .awaitTermination()