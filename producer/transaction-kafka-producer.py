from confluent_kafka import Producer
import json
import random
import time
import signal
from datetime import datetime, timedelta
from faker import Faker
import uuid
from datetime import datetime, timezone

fake = Faker()
random.seed(42)

running = True

customers = {
    "Alice": ["card_1", "card_2"],
    "Bob": ["card_3"],
    "Charlie": ["card_4", "card_5"],
    "David": ["card_6"],
    "Eva": ["card_7", "card_8"],
}

locations = ["NY", "CA", "TX", "FL", "IL"]
last_card_activity = {}
merchant_categories = {
    "ECOM": ["Amazon", "Ebay", "Shopify"],
    "POS": ["Walmart", "Target", "Costco"],
    "ATM": ["Chase ATM", "BoA ATM"],
}


def get_event_time(card_id):
    if card_id in last_card_activity:
        return last_card_activity[card_id]["time"] + timedelta(
            seconds=random.randint(1, 5)
        )
    return datetime.utcnow()


def generate_id():
    return ".".join(str(random.randint(1, 245)) for _ in range(4))  # anonymous loop


def generate_event_id():
    ts = int(datetime.now(timezone.utc).timestamp() * 1000)
    return f"evt-{ts}-{uuid.uuid4().hex[:6]}"


def generate_transaction():
    customer = random.choice(list(customers.keys()))
    card_id = random.choice(customers[customer])

    fraud_type = random.choices(
        ["NORMAL", "CARD_TESTING", "VELOCITY", "IMPOSSIBLE_TRAVEL"],
        weights=[80, 8, 6, 6],
    )[0]

    now = get_event_time(card_id)
    location = random.choice(locations)
    amount = round(random.uniform(20, 300), 2)
    category = random.choice(list(merchant_categories.keys()))
    merchant = random.choice(merchant_categories[category])