# Конфигурационные настройки приложения

# Kafka настройки
KAFKA_BOOTSTRAP_SERVERS = "rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091"
KAFKA_INPUT_TOPIC = "student.topic.cohort12.baidakova"
KAFKA_OUTPUT_TOPIC = "student.topic.cohort12.baidakova.out"
KAFKA_SECURITY_PROTOCOL = "SASL_SSL"
KAFKA_SASL_MECHANISM = "SCRAM-SHA-512"
KAFKA_CA_CERT_PATH = "/root/CA.pem"

# PostgreSQL настройки (локальная БД)
POSTGRES_LOCAL_URL = "jdbc:postgresql://localhost:5432/de"
POSTGRES_FEEDBACK_TABLE = "subscribers_feedback"
POSTGRES_DRIVER = "org.postgresql.Driver"

# PostgreSQL настройки (Yandex Cloud)
POSTGRES_CLOUD_URL = "jdbc:postgresql://rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de"
POSTGRES_SUBSCRIBERS_TABLE = "subscribers_restaurants"

# Spark настройки
SPARK_APP_NAME = "RestaurantSubscribeStreamingService"
SPARK_JARS_PACKAGES = ",".join([
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
    "org.postgresql:postgresql:42.4.0",
])
CHECKPOINT_LOCATION = "/tmp/checkpoint"
