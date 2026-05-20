import React, { useState } from "react";
import { createRoot } from "react-dom/client";
import axios from "axios";

function App() {
  const [name, setName] = useState("");
  const [message, setMessage] = useState("");
  const [status, setStatus] = useState("");

  const sendMessage = async (e) => {
    e.preventDefault();

    try {
      setStatus("Sending message...");

      await axios.post("http://localhost:5000/api/messages", {
        name,
        message,
      });

      setStatus("Message sent to Kafka successfully!");
      setName("");
      setMessage("");
    } catch (error) {
      console.error(error);
      setStatus("Failed to send message");
    }
  };

  return (
    <div style={styles.container}>
      <h1>3-Tier Kafka Demo App</h1>

      <p>
        Frontend → Backend API → Kafka → Consumer → PostgreSQL
      </p>

      <form onSubmit={sendMessage} style={styles.form}>
        <input
          style={styles.input}
          type="text"
          placeholder="Enter your name"
          value={name}
          onChange={(e) => setName(e.target.value)}
        />

        <textarea
          style={styles.textarea}
          placeholder="Enter message"
          value={message}
          onChange={(e) => setMessage(e.target.value)}
        />

        <button style={styles.button} type="submit">
          Send Message
        </button>
      </form>

      <h3>{status}</h3>
    </div>
  );
}

const styles = {
  container: {
    maxWidth: "600px",
    margin: "80px auto",
    fontFamily: "Arial",
    textAlign: "center",
  },
  form: {
    display: "flex",
    flexDirection: "column",
    gap: "15px",
  },
  input: {
    padding: "12px",
    fontSize: "16px",
  },
  textarea: {
    padding: "12px",
    fontSize: "16px",
    height: "100px",
  },
  button: {
    padding: "12px",
    fontSize: "16px",
    cursor: "pointer",
  },
};

createRoot(document.getElementById("root")).render(<App />);
