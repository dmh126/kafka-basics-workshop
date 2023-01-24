from confluent_kafka import Producer

producer = Producer({'bootstrap.servers': 'localhost:9092'})

def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

producer.poll(0)

# produce message
data = "Hello BM!"
producer.produce('mytopic', data.encode('utf-8'), callback=delivery_report)

# wait for the callback
producer.flush()