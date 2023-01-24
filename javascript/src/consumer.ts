import { Kafka, EachMessagePayload } from 'kafkajs';
import config from './config.json';

const client = new Kafka({
  clientId: 'test-app',
  brokers: [config.broker]
});

const processMessage = async ({ topic, partition, message}: EachMessagePayload) => {
  console.log(`Received a message from ${topic} [${partition}]: ${message.value.toString()}`)
}

async function consumeMessages(kafkaClient: Kafka) {
  // creste consumer
  const consumer = kafkaClient.consumer({ groupId: 'test-group' });
  await consumer.connect();

  // subscribe to a topic
  await consumer.subscribe({
    topic: 'test_topic',
    fromBeginning: true,
  });

  await consumer.run({
    eachMessage: processMessage,
  })

}

// RUN
(async() => {
  await consumeMessages(client);
})();