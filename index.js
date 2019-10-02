require('dotenv').config();

const fs = require('fs');
const { KafkaClient, Consumer } = require('kafka-node');

const { KAFKA_HOST = "", KAFKA_TOPIC = "" } = process.env;

// Create a new consumer connection
const client = new KafkaClient({kafkaHost: KAFKA_HOST});
const consumer = new Consumer(client, [{ topic: KAFKA_TOPIC }]);

// On reading a message from kafka, write the content to file
consumer.on('message', message => {
  const fileName = `${KAFKA_TOPIC}_${Date.now()}.json`;

  fs.writeFile(fileName, message.value, err => {
    if (err) {
      // Log error to logger or console
      console.error('CONSUMER: Error writing message to file:\n', err);
    }
  });
});

consumer.on('error', err => {
  // Log error to logger or console
  console.error('CONSUMER: An error occurred:\n', err);
});
