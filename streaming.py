import os
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, struct, current_timestamp, unix_timestamp
from pyspark.sql.types import StructType, StructField, StringType, LongType

# Путь к сертификату — из переменной окружения или из контейнера
ca_cert_path = os.environ.get('KAFKA_CA_CERT', '/root/CA.pem')

# метод для записи данных в 2 target: в PostgreSQL для фидбэков и в Kafka для триггеров
def foreach_batch_function(df, epoch_id):
    # сохраняем df в памяти, чтобы не создавать df заново перед отправкой в Kafka
    df.persist()
    
    # записываем df в PostgreSQL с полем feedback
    df.select(
        'restaurant_id', 'adv_campaign_id', 'adv_campaign_content',
        'adv_campaign_owner', 'adv_campaign_owner_contact',
        'adv_campaign_datetime_start', 'adv_campaign_datetime_end',
        'datetime_created', 'client_id', 'trigger_datetime_created'
    ).write \
        .format('jdbc') \
        .option('url', 'jdbc:postgresql://localhost:5432/de') \
        .option('driver', 'org.postgresql.Driver') \
        .option('dbtable', 'subscribers_feedback') \
        .option('user', 'jovyan') \
        .option('password', 'jovyan') \
        .mode('append') \
        .save()
    
    # создаём df для отправки в Kafka. Сериализация в json.
    kafka_df = df.select(
        'restaurant_id', 'adv_campaign_id', 'adv_campaign_content',
        'adv_campaign_owner', 'adv_campaign_owner_contact',
        'adv_campaign_datetime_start', 'adv_campaign_datetime_end',
        'client_id', 'datetime_created', 'trigger_datetime_created'
    )
    
    kafka_with_value = kafka_df.withColumn(
        'value',
        to_json(struct(
            'restaurant_id', 'adv_campaign_id', 'adv_campaign_content',
            'adv_campaign_owner', 'adv_campaign_owner_contact',
            'adv_campaign_datetime_start', 'adv_campaign_datetime_end',
            'client_id', 'datetime_created', 'trigger_datetime_created'
        )).cast('string')
    ).select('value')
    
    # отправляем сообщения в результирующий топик Kafka без поля feedback
    kafka_with_value.write \
        .format('kafka') \
        .option('kafka.bootstrap.servers', 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091') \
        .option('topic', 'student.topic.cohort12.baidakova.out') \
        .option('kafka.security.protocol', 'SASL_SSL') \
        .option('kafka.sasl.jaas.config', 
                'org.apache.kafka.common.security.scram.ScramLoginModule required username="de-student" password="ltcneltyn";') \
        .option('kafka.sasl.mechanism', 'SCRAM-SHA-512') \
        .option('kafka.ssl.ca.location', ca_cert_path) \
        .save()
    
    # очищаем память от df
    df.unpersist()

# необходимые библиотеки для интеграции Spark с Kafka и PostgreSQL
spark_jars_packages = ",".join([
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
    "org.postgresql:postgresql:42.4.0",
])

# создаём spark сессию
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
    .option('kafka.sasl.jaas.config', 
            'org.apache.kafka.common.security.scram.ScramLoginModule required username="de-student" password="ltcneltyn";') \
    .option('kafka.sasl.mechanism', 'SCRAM-SHA-512') \
    .option('kafka.ssl.ca.location', ca_cert_path) \
    .option('subscribe', 'student.topic.cohort12.baidakova') \
    .load()

# определяем схему входного сообщения для json
incomming_message_schema = StructType([
    StructField("restaurant_id", StringType(), True),
    StructField("adv_campaign_id", StringType(), True),
    StructField("adv_campaign_content", StringType(), True),
    StructField("adv_campaign_owner", StringType(), True),
    StructField("adv_campaign_owner_contact", StringType(), True),
    StructField("adv_campaign_datetime_start", LongType(), True),
    StructField("adv_campaign_datetime_end", LongType(), True),
    StructField("datetime_created", LongType(), True)
])

# определяем текущее время в UTC в секундах
current_timestamp_utc = int(round(datetime.now(timezone.utc).timestamp()))

# десериализуем из value сообщения json и фильтруем по времени старта и окончания акции
filtered_read_stream_df = restaurant_read_stream_df \
    .select(from_json(col("value").cast("string"), incomming_message_schema).alias("parsed_value")) \
    .select("parsed_value.*") \
    .filter(
        (col("adv_campaign_datetime_start") <= current_timestamp_utc) & 
        (col("adv_campaign_datetime_end") >= current_timestamp_utc)
    )

# вычитываем всех пользователей с подпиской на рестораны
subscribers_restaurant_df = spark.read \
    .format('jdbc') \
    .option('url', 'jdbc:postgresql://rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de') \
    .option('driver', 'org.postgresql.Driver') \
    .option('dbtable', 'subscribers_restaurants') \
    .option('user', 'jovyan') \
    .option('password', 'jovyan') \
    .load()

# джойним данные из сообщения Kafka с пользователями подписки по restaurant_id (uuid). Добавляем время создания события.
result_df = filtered_read_stream_df \
    .join(subscribers_restaurant_df, ['restaurant_id'], 'inner') \
    .withColumn('trigger_datetime_created', unix_timestamp(current_timestamp()))

# запускаем стриминг
result_df.writeStream \
    .foreachBatch(foreach_batch_function) \
    .outputMode("update") \
    .option("checkpointLocation", "/tmp/checkpoint") \
    .start() \
    .awaitTermination()
