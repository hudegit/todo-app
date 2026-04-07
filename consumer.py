import json
import os
import psycopg2
from confluent_kafka import Consumer, KafkaException
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database connection
def get_db():
    return psycopg2.connect(
        host=os.environ.get("DB_HOST"),
        database=os.environ.get("DB_NAME"),
        user=os.environ.get("DB_USER"),
        password=os.environ.get("DB_PASSWORD"),
        port=os.environ.get("DB_PORT", "5432")
    )

# Kafka consumer setup
conf = {
    'bootstrap.servers': os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    'group.id': 'todo-consumer-group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)
consumer.subscribe(['todo_tasks'])

def process_message(message):
    try:
        db = get_db()
        cursor = db.cursor()

        data = json.loads(message.value().decode('utf-8'))
        action = data.get('action')

        if action == 'create':
            cursor.execute(
                "INSERT INTO todos (task, done) VALUES (%s, %s)",
                (data['task'], data['done'])
            )
            print(f"Created task: {data['task']}")

        elif action == 'toggle':
            cursor.execute(
                "UPDATE todos SET done = NOT done WHERE id = %s",
                (data['id'],)
            )
            print(f"Toggled task with ID: {data['id']}")

        elif action == 'delete':
            cursor.execute(
                "DELETE FROM todos WHERE id = %s",
                (data['id'],)
            )
            print(f"Deleted task with ID: {data['id']}")

        db.commit()
        cursor.close()
        db.close()

    except Exception as e:
        print(f"Error processing message: {e}")

def consume_messages():
    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaException._PARTITION_EOF:
                    continue
                else:
                    print(f"Consumer error: {msg.error()}")
                    continue

            print(f"Received message: {msg.value().decode('utf-8')}")
            process_message(msg)

    except KeyboardInterrupt:
        print("Consumer stopped by user")
    finally:
        consumer.close()

if __name__ == "__main__":
    print("Starting Kafka consumer...")
    consume_messages()