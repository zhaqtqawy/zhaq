# Simple Telegram Bot

A simple Telegram bot that responds to user messages with a fixed response.

## Setup

1. Install the required dependencies:
```bash
pip install -r requirements.txt
```

2. Create a `.env` file in the project root and add your bot token:
```
BOT_TOKEN=your_bot_token_here
```

3. Get your API credentials:
   - Go to https://my.telegram.org/auth
   - Log in with your phone number
   - Go to 'API development tools'
   - Create a new application
   - Copy the `api_id` and `api_hash`

4. Update the `bot.py` file with your API credentials:
   - Replace `api_id=1` with your actual API ID
   - Replace `'your_api_hash'` with your actual API hash

## Running the Bot

Run the bot using:
```bash
python bot.py
```

## Features

- Responds to /start command with a welcome message
- Responds to any text message with a fixed response
- Ignores other commands

## Note

Make sure to keep your bot token and API credentials secure and never share them publicly. 