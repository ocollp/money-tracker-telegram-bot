const TelegramBot = require('node-telegram-bot-api');

function createBot() {
  const token = process.env.TELEGRAM_BOT_TOKEN;
  if (!token) {
    throw new Error('Missing TELEGRAM_BOT_TOKEN in .env');
  }
  return new TelegramBot(token, { polling: false });
}

module.exports = { createBot };
