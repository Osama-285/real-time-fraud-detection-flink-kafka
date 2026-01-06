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
BOOTSTRAP_SERVERS = "localhost:9094"
TOPIC_NAME = "transactions"

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
    return datetime.now(timezone.utc)


def generate_ip():
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
    
    if fraud_type == "CARD_TESTING":
        amount = round(random.uniform(1, 5), 2)
        
    elif fraud_type == "VELOCITY":
        amount = round(random.uniform(80, 200), 2)
        
    elif fraud_type == "IMPOSSIBLE_TRAVEL":
        if card_id in last_card_activity:
            prev_loc = last_card_activity[card_id]["location"]
            location = random.choice([l for l in locations if l != prev_loc])
            now = last_card_activity[card_id]["time"] + timedelta(seconds=90)
            
    transaction = {
        "schema_version": "1.0",
        "transaction_id": fake.uuid4(),
        "event_id": generate_event_id(),
        "customer_id": customer,
        "card_id": card_id,
        "merchant_id": merchant.replace(" ", "_").lower(),
        "merchant_category": category,
        "amount": amount,
        "currency": "USD",
        "ip_address": generate_ip(),
        "location": location,
        "event_type": fraud_type,
        "timestamp": now.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
    }

    last_card_activity[card_id] = {
        "location": location,
        "time": now
    }

    return transaction, fraud_type

def shutdown(sig, frame):
    global running
    running = False
    
def delivery_report(err, msg):
    if err:
        print(f" Delivery failed: {err}")

def main():
    producer = Producer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "acks":"1",
            "linger.ms":10
        }
    )
    
    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    print("Kafka producer started")

    while running:
        tx, fraud = generate_transaction()

        producer.produce(
            topic=TOPIC_NAME,
            key=tx["card_id"],
            value=json.dumps(tx),
            headers={
                "event_type": fraud,
                "schema_version": "1.0",
            },
            on_delivery=delivery_report,
        )

        producer.poll(0)

        print(f"[{fraud}] {tx['card_id']} | ${tx['amount']} | {tx['location']}")

        time.sleep(0.4 if fraud != "VELOCITY" else 0.15)

    producer.flush()
    
if __name__ == "__main__":
    main()