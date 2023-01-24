import { Kafka } from 'kafkajs';
import { SchemaRegistry } from '@kafkajs/confluent-schema-registry';
import config from './config.json';

const client = new Kafka({
  clientId: 'test-app',
  brokers: [config.broker],
});
const registry = new SchemaRegistry({ host: config.schemaRegistry });


async function produceMessage(kafkaClient: Kafka) {
  // get producer
  const producer = kafkaClient.producer();
  await producer.connect();
  // get schema id
  const id = await registry.getLatestSchemaId("test_topic-value");
  // encode payload
  const msg_value = { id: 5 };
  const encoded_value = await registry.encode(id, msg_value);

  // send message
  await producer.send({
    topic: 'test_topic',
    messages: [
      { key: 'fakeId', value: encoded_value}
    ]
  })

  // disconnect
  await producer.disconnect();
}

// RUN
(async() => {
  await produceMessage(client)
})()