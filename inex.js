'use strict';

const { Kafka } = require('kafkajs');

const kafka1 = new Kafka({
  clientId: 'demo-client1',
  brokers: ['kafka.labs.wise-paas.com:19092', 'kafka.labs.wise-paas.com:19093', 'kafka.labs.wise-paas.com:19094'],
  sasl: {
    mechanism: 'scram-sha-256',
    username: 'BEoKRe50q5Z9',
    password: '96ad5d5ab3ad69a80b1dff14b4e9c1a8'
  }
});

const kafka2 = new Kafka({
  clientId: 'demo-client2',
  brokers: ['kafka.labs.wise-paas.com:19092', 'kafka.labs.wise-paas.com:19093', 'kafka.labs.wise-paas.com:19094'],
  sasl: {
    mechanism: 'scram-sha-256',
    username: 'BEoKRe50q5Z9',
    password: '96ad5d5ab3ad69a80b1dff14b4e9c1a8'
  }
});

const topic = 'BEoKRe50q5Z9-status';
const producer = kafka1.producer();
const consumer = kafka2.consumer({ groupId: 'BEoKRe50q5Z9-group' });

async function run () {
  await producer.connect();
  console.log('producer connected');
  await consumer.connect();
  console.log('consumer connected');
  await consumer.subscribe({ topic: topic, fromBeginning: true });
  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log({ value: message.value.toString() });
    }
  });

  await producer.send({ topic: topic, messages: [{ value: 'Hello World !' }] });
}

run();
