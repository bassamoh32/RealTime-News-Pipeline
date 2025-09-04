import os
import sys
import requests
import time
import json
from kafka import KafkaProducer
from dotenv import load_dotenv
import yaml
import logging

# Load environment variables from .env file
load_dotenv()

# Add parent directory to sys.path (if needed)
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

logger = logging.getLogger(__name__)

NEWS_API_KEY = os.getenv('NEWS_API_KEY')
if not NEWS_API_KEY:
    logger.error("NEWS_API_KEY not set in environment variables!")
    sys.exit(1)
# Load configurations
def load_config(config_file):
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'configuration', config_file)
    with open(path) as f:
        return yaml.safe_load(f)

kafka_config = load_config('kafka.yml')
api_config = load_config('source_api.yml')
NEWS_API_URL = api_config['NEWS_API_URL']
CATEGORIES = api_config['CATEGORIES']
BROKER = kafka_config['BROKER']
TOPIC = kafka_config['KAFKA_TOPIC']

try:
    producer = KafkaProducer(
        bootstrap_servers=BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        api_version=(2, 7, 0)
    )
except Exception as e:
    logger.error(f"Failed to create KafkaProducer: {e}")
    sys.exit(1)


def fetch_news(category, country="us"):
    params = {
        "apiKey": NEWS_API_KEY,
        "category": category,
        "country": country,
        "pageSize": 5
    }
    try:
        response = requests.get(NEWS_API_URL, params=params)
        response.raise_for_status()
        data = response.json()
        return data.get("articles", [])
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching category {category}: {e}")
        return []


def stream_news():
    while True:
        for category in CATEGORIES:
            logger.info(f"Fetching category: {category}")
            articles = fetch_news(category=category)
            if not articles:
                logger.warning(f"No articles found for category {category}")
            for article in articles:
                message = {
                    "title": article.get("title"),
                    "description": article.get("description"),
                    "url": article.get("url"),
                    "publishedAt": article.get("publishedAt"),
                    "source": article.get("source", {}).get("name"),
                    "category": category
                }
                logger.info(f"Sending: {message['title']} ({category})")
                try:
                    producer.send(TOPIC, value=message)
                    producer.flush() # flush after each send for reliability
                except Exception as e:
                    logger.error(f"Failed to send message to Kafka: {e}")
                time.sleep(2)
            time.sleep(60)


if __name__ == "__main__":
    try:
        stream_news()
    except KeyboardInterrupt:
        logger.info("Stopped by user")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
