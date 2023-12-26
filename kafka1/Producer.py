from kafka import KafkaProducer
producer = KafkaProducer(bootstrap_servers='localhost:9092')
aaa = 'han'
# Send some test events
producer.send('sample', b'Hello, kim2')
producer.send('sample1', b'Hello, FISA222')
producer.send('kim', b'Hello, kimqeongsss')
producer.send(aaa, b'Hello, han_START')

producer.send('sample1', key=b'message-two', value=b'Kafka in use!')
producer.flush()