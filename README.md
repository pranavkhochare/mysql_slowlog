# MySQL Slow Query Log Analyzer

This Python script analyzes MySQL slow query logs to identify and optimize problematic queries using AI-powered recommendations. It combines slow query logs from the previous day, analyzes them using `pt-query-digest`, and provides optimization suggestions using a language model.

## Features

- Automatically collects and combines MySQL slow query logs from the previous day
- Analyzes slow SELECT queries using `pt-query-digest`
- Generates AI-powered optimization suggestions using Ollama
- Sends notifications via Microsoft Teams
- Supports email reporting with both HTML and plain text formats
- Includes detailed logging for troubleshooting

## Prerequisites

- Python 3.x
- MySQL Server
- Percona Toolkit (`pt-query-digest`)
- Ollama LLM server (for AI-powered suggestions)

## Installation

1. Clone this repository
2. Install the required Python packages:
```bash
pip install -r requirements.txt
```

## Configuration

Create a `.env` file in the project directory with the following variables:

```env
# Database Configuration
db_username=your_mysql_username
db_password=your_mysql_password
db_hostname=your_mysql_host

# LLM Configuration
LLM_API_URL=your_ollama_api_url
LLM_MODEL=your_model_name

# Email Configuration
HOST=your_smtp_host
PORT=your_smtp_port
SENDER=sender@example.com
MAIL_USER=your_mail_username
PASSWORD=your_mail_password

# Teams Webhook (Optional)
WEBHOOK_URL=your_teams_webhook_url
```

## Usage

Run the script:

```bash
python analyze_slowlog.py
```

The script will:
1. Collect slow query logs from the previous day
2. Combine and analyze them using pt-query-digest
3. Generate optimization suggestions using AI
4. Send reports via email and Teams (if configured)

## Output

- Generates a combined slow query log file
- Creates a JSON file with filtered and sorted slow queries
- Sends email reports with query analysis
- Posts notifications to Microsoft Teams (if configured)
- Maintains detailed logs in the `./logs` directory

## Logging

Logs are stored in `./logs/analyze-mysql-slow.log` with detailed information including:
- Timestamp
- Function name
- Thread name
- Log level
- Message

## Error Handling

The script includes comprehensive error handling for:
- Missing configuration
- Database connection issues
- File operations
- External tool failures (pt-query-digest)
- API communication (Teams, Ollama)

## Author

[Pranav khochare]
