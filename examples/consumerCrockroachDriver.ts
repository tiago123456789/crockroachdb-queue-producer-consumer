import { config } from "dotenv";
config();

import Consumer from "../src/consumer";
import CrockroachQueueDriver from "../src/queueDriver/CrockroachQueueDriver";
import knex from "knex";

async function start() {
  const knexInstance = knex({
    client: "cockroachdb",
    connection: {
      host: process.env.CROCKROACH_HOST,
      database: process.env.CROCKROACH_DATABASE,
      user: process.env.CROCKROACH_USER,
      password: process.env.CROCKROACH_PASSWORD,
      port: Number(process.env.CROCKROACH_PORT) || 26257,
      ssl: true,
    },
  });

  const queueDriver = new CrockroachQueueDriver(knexInstance, "pgmq");

  console.time("send-messages");
  let messages: { [key: string]: any }[] = [];
  let itemsPerMessage = [];
  for (let index = 0; index < 1000000; index++) {
    itemsPerMessage.push({ message: "Hello, CrockroachDB!" });

    if (itemsPerMessage.length == 10) {
      messages.push({ items: itemsPerMessage });
      itemsPerMessage = [];
    }

    if (messages.length == 4000) {
      console.log(`Sending message ${messages.length}`);
      await queueDriver.sendBatch("jobs", messages);
      messages = [];
    }
  }

  if (messages.length > 0) {
    console.log(`Sending message ${messages.length}`);
    await queueDriver.sendBatch("jobs", messages);
  }

  console.timeEnd("send-messages");

  console.log("Finished sending messages");
  // await queueDriver.sendBatch(
  //   "jobs",
  //   [{ message: "test" }],
  //   new AbortController().signal
  // );

  // const consumer = new Consumer(
  //   {
  //     queueName: "jobs",
  //     visibilityTime: 5,
  //     consumeType: "read",
  //     poolSize: 20,
  //     timeMsWaitBeforeNextPolling: 1000,
  //     enabledPolling: true,
  //     queueNameDlq: "jobs_dlq",
  //     totalRetriesBeforeSendToDlq: 2,
  //   },
  //   async function (message: { [key: string]: any }, signal): Promise<void> {
  //     console.log("Processing message =>", JSON.stringify(message));
  //   },
  //   queueDriver
  // );

  // consumer.on("send-to-dlq", (message: { [key: string]: any }) => {
  //   console.log("Send to DLQ =>", message);
  // });

  // consumer.on("error", (err: Error) => {
  //   console.error("Error consuming message:", err.message);
  // });

  // await consumer.start();

  // process.on("SIGINT", async () => {
  //   await knexInstance.destroy();
  //   process.exit(0);
  // });
}

start();
