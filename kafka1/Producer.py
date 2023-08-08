from kafka import KafkaProducer
producer = KafkaProducer(bootstrap_servers='localhost:9092')

# Send some test events
producer.send('sample', b'Hello, FISA')
producer.send('sample1', b'Hello, FISA')
producer.send('sample1', key=b'message-two', value=b'Kafka in use!')
producer.flush()