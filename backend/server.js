const express = require("express");
const cors = require("cors");
const { Kafka } = require("kafkajs");

const app = express();

app.use(cors());
app.use(express.json());

const PORT = process.env.PORT || 5000;
const KAFKA_BROKER = process.env.KAFKA_BROKER || "localhost:9092";
const KAFKA_TOPIC = process.env.KAFKA_TOPIC || "user-messages";

const kafka = new Kafka({
  clientId: "backend-producer",
  brokers: [KAFKA_BROKER],
});

const producer = kafka.producer();

async function connectProducer() {
  let retries = 10;

  while (retries > 0) {
    try {
      await producer.connect();
      console.log("Kafka producer connected");
      return;
    } catch (error) {
      console.log("Kafka producer connection failed. Retrying...");
      retries--;
      await new Promise((resolve) => setTimeout(resolve, 5000));
    }
  }

  console.error("Could not connect to Kafka producer");
  process.exit(1);
}

app.get("/", (req, res) => {
  res.send("Backend API is running");
});

app.post("/api/messages", async (req, res) => {
  try {
    const { name, message } = req.body;

    if (!name || !message) {
      return res.status(400).json({
        success: false,
        error: "Name and message are required",
      });
    }

    const payload = {
      name,
      message,
      createdAt: new Date().toISOString(),
    };

    await producer.send({
      topic: KAFKA_TOPIC,
      messages: [
        {
          key: name,
          value: JSON.stringify(payload),
        },
      ],
    });

    res.status(200).json({
      success: true,
      message: "Message sent to Kafka successfully",
      data: payload,
    });
  } catch (error) {
    console.error("Error sending message to Kafka:", error);
    res.status(500).json({
      success: false,
      error: "Failed to send message to Kafka",
    });
  }
});

app.listen(PORT, async () => {
  console.log(`Backend running on port ${PORT}`);
  await connectProducer();
});
