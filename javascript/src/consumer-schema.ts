import { Kafka, EachMessagePayload } from 'kafkajs';
import { SchemaRegistry } from '@kafkajs/confluent-schema-registry';
import config from './config.json';

const client = new Kafka({
  clientId: 'test-app',
  brokers: [config.broker]
});
const registry = new SchemaRegistry({ host: config.schemaRegistry })

const processMessage = async ({ topic, partition, message}: EachMessagePayload) => {
  try {
    const msg = await registry.decode(message.value);
    console.log(`Decoded message: ${JSON.stringify(msg)}`);
  } catch(err) {
    console.log(err)
  }
}

async function consumeMessages(kafkaClient: Kafka) {
  // creste consumer
  const consumer = kafkaClient.consumer({ groupId: 'test-group-schema' });
  await consumer.connect();

  // subscribe to a topic
  await consumer.subscribe({
    topic: 'test_topic',
  });

  await consumer.run({
    eachMessage: processMessage,
  })

}

// RUN
(async() => {
  await consumeMessages(client);
})();