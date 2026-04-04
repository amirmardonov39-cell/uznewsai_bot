# Uzbekistan Legal News Bot

This is a Telegram Bot with a dual-persona expert (Tech Blogger and Uzbekistan Law Professor).
It analyzes news links and provides legal insights based on the recent Uzbekistan's digital legislation.

## Setup Instructions

1. **Obtain API Keys**:
   - Create a Telegram bot using [BotFather](https://t.me/BotFather) and get your `TELEGRAM_TOKEN`.
   - Get an OpenAI API Key from your [OpenAI Platform](https://platform.openai.com/api-keys).

2. **Configure Environment Variables**:
   - In this directory, you will find a file named `.env.example`.
   - Rename it to `.env`:
     ```bash
     mv .env.example .env
     ```
   - Open `.env` in a text editor and paste your API keys:
     ```env
     TELEGRAM_TOKEN=your_token_here
     OPENAI_API_KEY=your_key_here
     ```

3. **Run the Bot**:
   - Ensure the virtual environment is activated:
     ```bash
     source venv/bin/activate
     ```
   - Start the bot:
     ```bash
     python bot.py
     ```

## Features
- **Send News Links**: Simply paste a URL to any news article, and the bot will fetch the content and analyze it.
- **Law Query**: Use the `/check_law` command with your query to get an answer strictly bounded by the latest digital legislation of Uzbekistan.
- **Duplicate Prevention**: The bot creates a `bot_database.sqlite` file to ensure the exact same URL isn't processed more than once.
