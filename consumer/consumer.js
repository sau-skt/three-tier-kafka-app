const { Kafka } = require("kafkajs");
const { Pool } = require("pg");

const KAFKA_BROKER = process.env.KAFKA_BROKER || "localhost:9092";
const KAFKA_TOPIC = process.env.KAFKA_TOPIC || "user-messages";

const kafka = new Kafka({
  clientId: "message-consumer",
  brokers: [KAFKA_BROKER],
});

const consumer = kafka.consumer({
  groupId: "message-consumer-group",
});

const pool = new Pool({
  host: process.env.PGHOST || "localhost",
  port: process.env.PGPORT || 5432,
  user: process.env.PGUSER || "appuser",
  password: process.env.PGPASSWORD || "apppass",
  database: process.env.PGDATABASE || "kafka_demo",
});

async function wait(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function connectPostgres() {
  let retries = 10;

  while (retries > 0) {
    try {
      await pool.query("SELECT 1");
      console.log("PostgreSQL connected");
      return;
    } catch (error) {
      console.log("PostgreSQL connection failed. Retrying...");
      retries--;
      await wait(5000);
    }
  }

  console.error("Could not connect to PostgreSQL");
  process.exit(1);
}

async function startConsumer() {
  await connectPostgres();

  let retries = 10;

  while (retries > 0) {
    try {
      await consumer.connect();
      console.log("Kafka consumer connected");
      break;
    } catch (error) {
      console.log("Kafka consumer connection failed. Retrying...");
      retries--;
      await wait(5000);
    }
  }

  await consumer.subscribe({
    topic: KAFKA_TOPIC,
    fromBeginning: true,
  });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const value = message.value.toString();
      const data = JSON.parse(value);

      console.log("Received message from Kafka:", data);

      await pool.query(
        "INSERT INTO messages(name, message) VALUES($1, $2)",
        [data.name, data.message]
      );

      console.log("Message saved to PostgreSQL");
    },
  });
}

startConsumer().catch((error) => {
  console.error("Consumer error:", error);
  process.exit(1);
});
