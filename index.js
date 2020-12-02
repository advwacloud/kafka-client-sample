'use strict';
// simulate the production env
require('dotenv').config();

const { Kafka } = require('kafkajs');

const kafkaBindingInfo = JSON.parse(process.env.ENSAAS_SERVICES);
const kafkaCredentials = kafkaBindingInfo.credentials;

let brokers = kafkaCredentials.externalHosts.split(',').map(b => b.trim());
let username = kafkaCredentials.username;
let password = kafkaCredentials.password;

let topic = kafkaCredentials.topics.find(t => t === `${username}-status`);

let mechanism = kafkaCredentials.sasl.mechanism;

let groupPrefix = kafkaCredentials.groupPrefix.find(g => g.includes(topic));

// arbitrary string
let groupCustomizePart = 'group';

let groupId = groupPrefix + groupCustomizePart;

const kafka1 = new Kafka({
  clientId: 'demo-client1',
  brokers,
  sasl: {
    mechanism,
    username,
    password
  }
});

const kafka2 = new Kafka({
  clientId: 'demo-client2',
  brokers,
  sasl: {
    mechanism,
    username,
    password
  }
});

const producer = kafka1.producer();
const consumer = kafka2.consumer({ groupId });

async function run () {
  await producer.connect();
  console.log('producer connected');
  await consumer.connect();
  console.log('consumer connected');
  await consumer.subscribe({ topic, fromBeginning: true });
  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log({ value: message.value.toString() });
    }
  });

  await producer.send({ topic, messages: [{ value: 'Hello World !' }] });
}

run();
