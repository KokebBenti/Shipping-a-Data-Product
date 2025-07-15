# Use Python 3.8 base image
FROM python:3.8-slim

# Set working directory inside the container
WORKDIR /app

# Copy everything from the current folder to /app in container
COPY . .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Run your scraper script when container starts
CMD ["python", "Telegram_Scraper.py"]