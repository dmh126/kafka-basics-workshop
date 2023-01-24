import { Kafka } from 'kafkajs';
import config from './config.json';

const client = new Kafka({
  clientId: 'test-app',
  brokers: [config.broker],
});

async function produceMessage(kafkaClient: Kafka) {
  // get producer
  const producer = kafkaClient.producer();
  await producer.connect();

  // send message
  await producer.send({
    topic: 'test_topic',
    messages: [
      { key: 'fakeId', value: 'Hello BM!'}
    ]
  })

  // disconnect
  await producer.disconnect();
}

// RUN
(async() => {
  await produceMessage(client)
})()
