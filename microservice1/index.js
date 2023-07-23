const express = require('express');
const amqp = require('amqplib');
const winston = require('winston');
const path = require('path');

const logger = winston.createLogger({
  level: 'info',
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.printf(({ timestamp, level, message }) => {
      return `[${timestamp}] ${level}: ${message}`;
    })
  ),
  transports: [
    new winston.transports.Console(),
    new winston.transports.File({ filename: path.join(__dirname, '../logs/microservice2.log') }),
  ],
});

const app = express();
const QUEUE_NAME = 'task_queue';
const RESULT_QUEUE_NAME = 'result_queue';

let connection;
let channel;

async function sendTaskToRabbitMQ(task) {
  try {
    if (!connection) {
      connection = await amqp.connect('amqp://localhost');
      channel = await connection.createChannel();
      await channel.assertQueue(QUEUE_NAME, { durable: true });
    }

    channel.sendToQueue(QUEUE_NAME, Buffer.from(task), { persistent: true });
    logger.info(`Задание отправлено в RabbitMQ: ${task}`);
  } catch (error) {
    logger.error(`Не удалось отправить задание в RabbitMQ: ${error.message}`);
    throw error;
  }
}

async function processResultFromRabbitMQ(result) {
  // Здесь может быть ваша логика обработки результата из RabbitMQ
  logger.info(`Получен результат из RabbitMQ: ${result}`);
}

async function receiveAndProcessResult() {
  try {
    if (!connection) {
      connection = await amqp.connect('amqp://localhost');
      channel = await connection.createChannel();
      await channel.assertQueue(RESULT_QUEUE_NAME, { durable: true });
    }

    logger.info('Ожидание результатов в очереди...');
    channel.consume(RESULT_QUEUE_NAME, (message) => {
      if (message !== null) {
        const result = message.content.toString();
        logger.info(`Получен результат из RabbitMQ: ${result}`);
        processResultFromRabbitMQ(result);
        channel.ack(message);
      }
    });
  } catch (error) {
    logger.error(`Не удалось получить и обработать результат: ${error.message}`);
  }
}

app.get('/', (req, res) => {
  const task = JSON.stringify({ path: req.path, method: req.method, query: req.query });

  sendTaskToRabbitMQ(task)
    .then(() => {
      res.status(200).json({ message: 'Запрос успешно получен. Обработка задания запущена.' });
    })
    .catch((error) => {
      logger.error(`Ошибка при обработке запроса: ${error.message}`);
      res.status(500).json({ message: 'Внутренняя ошибка сервера.' });
    });
});

const port = 3000;
app.listen(port, () => {
  logger.info(`Микросервис М1 запущен и слушает порт ${port}.`);
});

// При старте микросервиса М1, запускаем получение и обработку результатов из RabbitMQ
receiveAndProcessResult();
