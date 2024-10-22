const amqp = require("amqplib");
const fs = require("fs");
const createCsvWriter = require("csv-writer").createObjectCsvWriter;

const csvWriter = createCsvWriter({
  path: "rick_morty_characters.csv",
  header: [
    { id: "id", title: "ID" },
    { id: "name", title: "Name" },
    { id: "status", title: "Status" },
    { id: "species", title: "Species" },
    { id: "gender", title: "Gender" },
  ],
  append: true,
});

async function connect() {
  const conn = await amqp.connect("amqp://guest:guest@localhost:5672");
  const ch = await conn.createChannel();
  ch.prefetch(1);

  console.log("Conectado com Rabbit para Consumer");
  return ch;
}

async function saveToCsv(character) {
  try {
    await csvWriter.writeRecords([character]);
    console.log(`Personagem salvo no CSV: ${character.name}`);
  } catch (error) {
    console.error("Erro ao salvar no CSV:", error);
  }
}

function consumer(data, queue) {
  const result = JSON.parse(data.content.toString());

  //   saveToCsv(result);

  setTimeout(() => {
    saveToCsv(result);
    console.log(`Processado e salvo: ${result.name}`);
    queue.ack(data);
  }, 800);

  //   return queue.ack(data);
}

async function start() {
  const queue = await connect();

  let filaq = "RICKYEMORY";

  await queue.assertQueue(filaq, { durable: true });

  queue.consume(filaq, (data) => consumer(data, queue), {
    noAck: false,
  });
}

start();
