import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, struct, current_timestamp, unix_timestamp
from pyspark.sql.types import StructType, StructField, StringType, LongType

# Импортируем конфигурацию
import config
import secrets

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
        .option('url', config.POSTGRES_LOCAL_URL) \
        .option('driver', config.POSTGRES_DRIVER) \
        .option('dbtable', config.POSTGRES_FEEDBACK_TABLE) \
        .option('user', secrets.POSTGRES_USER) \
        .option('password', secrets.POSTGRES_PASSWORD) \
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
        .option('kafka.bootstrap.servers', config.KAFKA_BOOTSTRAP_SERVERS) \
        .option('topic', config.KAFKA_OUTPUT_TOPIC) \
        .option('kafka.security.protocol', config.KAFKA_SECURITY_PROTOCOL) \
        .option('kafka.sasl.jaas.config', 
                f'org.apache.kafka.common.security.scram.ScramLoginModule required username="{secrets.KAFKA_USERNAME}" password="{secrets.KAFKA_PASSWORD}";') \
        .option('kafka.sasl.mechanism', config.KAFKA_SASL_MECHANISM) \
        .option('kafka.ssl.ca.location', ca_cert_path) \
        .save()
    
    # очищаем память от df
    df.unpersist()

# создаём spark сессию
spark = SparkSession.builder \
    .appName(config.SPARK_APP_NAME) \
    .config("spark.sql.session.timeZone", "UTC") \
    .config("spark.jars.packages", config.SPARK_JARS_PACKAGES) \
    .getOrCreate()

# читаем из топика Kafka сообщения с акциями от ресторанов 
restaurant_read_stream_df = spark.readStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', config.KAFKA_BOOTSTRAP_SERVERS) \
    .option('kafka.security.protocol', config.KAFKA_SECURITY_PROTOCOL) \
    .option('kafka.sasl.jaas.config', 
            f'org.apache.kafka.common.security.scram.ScramLoginModule required username="{secrets.KAFKA_USERNAME}" password="{secrets.KAFKA_PASSWORD}";') \
    .option('kafka.sasl.mechanism', config.KAFKA_SASL_MECHANISM) \
    .option('kafka.ssl.ca.location', config.KAFKA_CA_CERT_PATH) \
    .option('subscribe', config.KAFKA_INPUT_TOPIC) \
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

# десериализуем из value сообщения json и фильтруем по времени старта и окончания акции
filtered_read_stream_df = restaurant_read_stream_df \
    .select(from_json(col("value").cast("string"), incomming_message_schema).alias("parsed_value")) \
    .select("parsed_value.*") \
    .filter(
        (col("adv_campaign_datetime_start") <= unix_timestamp(current_timestamp())) & 
        (col("adv_campaign_datetime_end") >= unix_timestamp(current_timestamp()))
    )

# вычитываем всех пользователей с подпиской на рестораны
subscribers_restaurant_df = spark.read \
    .format('jdbc') \
    .option('url', config.POSTGRES_CLOUD_URL) \
    .option('driver', config.POSTGRES_DRIVER) \
    .option('dbtable', config.POSTGRES_SUBSCRIBERS_TABLE) \
    .option('user', secrets.POSTGRES_CLOUD_USER) \
    .option('password', secrets.POSTGRES_CLOUD_PASSWORD) \
    .load()

# джойним данные из сообщения Kafka с пользователями подписки по restaurant_id (uuid). Добавляем время создания события.
result_df = filtered_read_stream_df \
    .join(subscribers_restaurant_df, ['restaurant_id'], 'inner') \
    .withColumn('trigger_datetime_created', unix_timestamp(current_timestamp()))

# запускаем стриминг
result_df.writeStream \
    .foreachBatch(foreach_batch_function) \
    .outputMode("append") \
    .option("checkpointLocation", config.CHECKPOINT_LOCATION) \
    .start() \
    .awaitTermination()
