import { Kafka, CompressionTypes } from 'kafkajs';

const host = 'host.docker.internal';

const kafka = new Kafka({
  brokers: [`${host}:9092`],
  clientId: 'example-producer',
})

const topic = 'topic-test'
const producer = kafka.producer()

const getRandomNumber = () => Math.round(Math.random(10) * 10)
const createMessage = num => ({
  key: `key-${num}`,
  value: `value-${num}-${new Date().toISOString()}`,
})

const sendMessage = () => {
    const randomNumber = getRandomNumber();
    const mapArray = Array(randomNumber).fill().map( _ => createMessage(randomNumber));
    console.log('kyle_debug ~ file: index.js ~ line 28 ~ sendMessage ~ mapArray', mapArray)
    return producer
        .send({
        topic,
        compression: CompressionTypes.GZIP,
        messages: mapArray,
        })
        .catch(e => console.error(`[example/producer] ${e.message}`, e))
}

const run = async () => {
  await producer.connect()
  setInterval(sendMessage, 3000)
}

run().catch(e => console.error(`[example/producer] ${e.message}`, e))

const errorTypes = ['unhandledRejection', 'uncaughtException']
const signalTraps = ['SIGTERM', 'SIGINT', 'SIGUSR2']

errorTypes.map(type => {
  process.on(type, async () => {
    try {
      console.log(`process.on ${type}`)
      await producer.disconnect()
      process.exit(0)
    } catch (_) {
      process.exit(1)
    }
  })
})

signalTraps.map(type => {
  process.once(type, async () => {
    try {
      await producer.disconnect()
    } finally {
      process.kill(process.pid, type)
    }
  })
})