from flask import Flask, render_template, request, redirect, url_for
import psycopg2
import os
import json
from confluent_kafka import Producer
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

app = Flask(__name__)

# Kafka producer setup
kafka_config = {
    'bootstrap.servers': os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
}
producer = Producer(kafka_config)

# Database connection
def get_db():
    return psycopg2.connect(
        host=os.environ.get("DB_HOST"),
        database=os.environ.get("DB_NAME"),
        user=os.environ.get("DB_USER"),
        password=os.environ.get("DB_PASSWORD"),
        port=os.environ.get("DB_PORT", "5432")
    )

# Create table if it doesn't exist
def init_db():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS todos (
            id      SERIAL PRIMARY KEY,
            task    TEXT NOT NULL,
            done    BOOLEAN DEFAULT FALSE,
            created TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    cur.close()
    conn.close()

# Helper function to send messages to Kafka
def send_to_kafka(action, task_id=None, task=None, done=None):
    message = {"action": action}
    if task_id is not None:
        message["id"] = task_id
    if task is not None:
        message["task"] = task
    if done is not None:
        message["done"] = done

    producer.produce(
        topic="todo_tasks",
        value=json.dumps(message).encode("utf-8")
    )
    producer.flush()

# Home page - show all todos
@app.route("/")
def index():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM todos ORDER BY created DESC")
    todos = cur.fetchall()
    cur.close()
    conn.close()
    return render_template("index.html", todos=todos)

# Add new todo
@app.route("/add", methods=["POST"])
def add():
    task = request.form.get("task")
    if task:
        # Send to Kafka instead of directly to DB
        send_to_kafka(action="create", task=task, done=False)
    return redirect(url_for("index"))

# Toggle todo done/undone
@app.route("/toggle/<int:id>")
def toggle(id):
    # Send to Kafka instead of directly to DB
    send_to_kafka(action="toggle", task_id=id)
    return redirect(url_for("index"))

# Delete todo
@app.route("/delete/<int:id>")
def delete(id):
    # Send to Kafka instead of directly to DB
    send_to_kafka(action="delete", task_id=id)
    return redirect(url_for("index"))

if __name__ == "__main__":
    init_db()
    app.run(host="0.0.0.0", port=8080, debug=True)