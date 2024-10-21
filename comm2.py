from kafka import KafkaProducer, KafkaConsumer, TopicPartition
from kafka.admin import KafkaAdminClient, NewTopic
import time

# Configurações
bootstrap_servers = ['localhost:9092', 'localhost:9093']  # Lista de brokers
topic_name = 'orders_topic'
num_partitions = 1
replication_factor = 2  # Fator de replicação maior que 1 para demonstrar replicação

# Criar tópico com replicação
admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
topic = NewTopic(name=topic_name, num_partitions=num_partitions, replication_factor=replication_factor)
admin_client.create_topics(new_topics=[topic], validate_only=False)

# Produtor de mensagens com garantias de entrega
producer = KafkaProducer(bootstrap_servers=bootstrap_servers, acks='all')  # 'all' para maior garantia
for i in range(10):
    message = f"Order {i} processed".encode('utf-8')
    producer.send(topic_name, message)
    time.sleep(1)

producer.flush()

# Consumidor de mensagens
consumer = KafkaConsumer(bootstrap_servers=bootstrap_servers,
                         auto_offset_reset='earliest',
                         consumer_timeout_ms=1000)
consumer.subscribe([topic_name])

print("Consumed messages:")
for message in consumer:
    print(f"Partition: {message.partition} | Message: {message.value.decode('utf-8')}")

# Limpeza
admin_client.delete_topics([topic_name])
