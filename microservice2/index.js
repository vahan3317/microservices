const amqp = require('amqplib');
const winston = require('winston');

const logger = winston.createLogger({
  level: 'info',
  format: winston.format.simple(),
  transports: [new winston.transports.Console()],
});

const TASK_QUEUE_NAME = 'task_queue';
const RESULT_QUEUE_NAME = 'result_queue';

let connection;
let channel;

async function sendResultToRabbitMQ(result) {
  try {
    if (!connection) {
      connection = await amqp.connect('amqp://localhost');
      channel = await connection.createChannel();
      await channel.assertQueue(RESULT_QUEUE_NAME, { durable: true });
    }

    channel.sendToQueue(RESULT_QUEUE_NAME, Buffer.from(result), { persistent: true });
    logger.info(`Результат отправлен в RabbitMQ: ${result}`);
  } catch (error) {
    logger.error(`Не удалось отправить результат в RabbitMQ: ${error.message}`);
  }
}

async function processTask(task) {
  try {
    logger.info(`Обработка задания: ${task}`);
    await new Promise((resolve) => setTimeout(resolve, 2000));
    const result = `Обработано: ${task}`;
    logger.info(`Результат: ${result}`);
    sendResultToRabbitMQ(result);
  } catch (error) {
    logger.error(`Ошибка при обработке задания: ${error.message}`);
  }
}

async function receiveAndProcessTask() {
  try {
    const connection = await amqp.connect('amqp://localhost');
    const channel = await connection.createChannel();
    await channel.assertQueue(TASK_QUEUE_NAME, { durable: true });

    logger.info('Ожидание задач в очереди...');
    channel.consume(TASK_QUEUE_NAME, (message) => {
      if (message !== null) {
        const task = message.content.toString();
        logger.info(`Получено задание из RabbitMQ: ${task}`);
        processTask(task)
          .then(() => channel.ack(message))
          .catch((error) => {
            logger.error(`Ошибка при обработке задания: ${error.message}`);
            channel.ack(message); // Подтверждаем получение сообщения даже в случае ошибки, чтобы избежать его застревания в очереди
          });
      }
    });
  } catch (error) {
    logger.error(`Не удалось получить и обработать задание: ${error.message}`);
  }
}

receiveAndProcessTask();
