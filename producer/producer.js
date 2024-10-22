const amqp = require("amqplib");
const axios = require("axios");

async function connect() {
  const conn = await amqp.connect("amqp://guest:guest@localhost:5672");
  const ch = await conn.createChannel();
  ch.prefetch(1);

  console.log("Conectado com Rabbit para Producer");
  return ch;
}

async function processPage(queue, filaq, url) {
  const response = await axios.get(url);
  const characters = response.data.results;
  const nextPage = response.data.info.next;

  for (let character of characters) {
    const envelope = JSON.stringify({
      id: character.id,
      name: character.name,
      status: character.status,
      species: character.species,
      gender: character.gender,
    });

    await queue.assertQueue(filaq, { durable: true });
    await queue.sendToQueue(filaq, Buffer.from(envelope), {
      persistent: true,
    });
  }

  if (nextPage) {
    await processPage(queue, filaq, nextPage);
  }
}

async function start() {
  const queue = await connect();

  let filaq = "RICKYEMORY";

  const mainurl = "https://rickandmortyapi.com/api/character";

  //   const response = await axios.get("https://rickandmortyapi.com/api/character");
  //   const characters = response.data.results;

  //   for (let character of characters) {
  //     const envelope = JSON.stringify({
  //       id: character.id,
  //       name: character.name,
  //       status: character.status,
  //       species: character.species,
  //       gender: character.gender,
  //     });

  //     await queue.assertQueue(filaq, { durable: true });
  //     await queue.sendToQueue(filaq, Buffer.from(envelope), { persistent: true });
  //   }

  await processPage(queue, filaq, mainurl);
}

start();
