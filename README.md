# Money Tracker Telegram Bot

Telegram bot to append expense/income entries to two Google Sheets (commands `/o` and `/a`).

## Commands

- **/o** `[DD/MM] amount [concept]` → append a row to Sheet 1 (SPREADSHEET_ID)
- **/a** `[DD/MM] amount [concept]` → append a row to Sheet 2 (SPREADSHEET_ID_2)

Examples:  
`/o 50` · `/o 15/02 30 indexa` · `/a 120 2`

## Setup

1. Copy `.env.example` to `.env`.
2. Fill in `TELEGRAM_BOT_TOKEN`, Google credentials (option A or B), `SPREADSHEET_ID` and `SPREADSHEET_ID_2`.
3. Share each Google Sheet with the Service Account email (Editor).

## Run

```bash
npm install
npm start
```

For production, set the same environment variables on your host (Railway, Render, etc.).
