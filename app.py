from flask import Flask, render_template, request, redirect, url_for
import psycopg2
import os
import json
import logging
from confluent_kafka import Producer
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Kafka producer setup
kafka_config = {
    'bootstrap.servers': os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka.test-kafka.svc.cluster.local:9092")
}
producer = Producer(kafka_config)

# Database connection
def get_db():
    try:
        conn = psycopg2.connect(
            host=os.environ.get("DB_HOST", "postgresql.test-kafka.svc.cluster.local"),
            database=os.environ.get("DB_NAME", "todo_db"),
            user=os.environ.get("DB_USER", "postgres"),
            password=os.environ.get("DB_PASSWORD", "postgres"),
            port=os.environ.get("DB_PORT", "5432")
        )
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to database: {str(e)}")
        raise

# Create table if it doesn't exist
def init_db():
    try:
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
        logger.info("Database initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize database: {str(e)}")
        raise

# Helper function to send messages to Kafka
def send_to_kafka(action, task_id=None, task=None, done=None):
    try:
        message = {"action": action}
        if task_id is not None:
            message["id"] = task_id
        if task is not None:
            message["task"] = task
        if done is not None:
            message["done"] = done

        producer.produce(
            topic=os.environ.get("KAFKA_TOPIC", "todo_tasks"),
            value=json.dumps(message).encode("utf-8")
        )
        producer.flush()
        logger.info(f"Successfully sent message to Kafka: {message}")
    except Exception as e:
        logger.error(f"Failed to send message to Kafka: {str(e)}")
        # Optionally, you could fall back to direct DB operations here

# Health check endpoint for Kafka
@app.route("/health/kafka")
def health_check_kafka():
    try:
        # Try to get metadata about the Kafka cluster
        cluster_metadata = producer.list_topics(timeout=10)
        return f"Kafka connection is healthy. Topics: {list(cluster_metadata.topics.keys())}"
    except Exception as e:
        return f"Kafka connection failed: {str(e)}", 500

# Health check endpoint for DB
@app.route("/health/db")
def health_check_db():
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.close()
        conn.close()
        return "Database connection is healthy"
    except Exception as e:
        return f"Database connection failed: {str(e)}", 500

# Home page - show all todos
@app.route("/")
def index():
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("SELECT * FROM todos ORDER BY created DESC")
        todos = cur.fetchall()
        cur.close()
        conn.close()
        return render_template("index.html", todos=todos)
    except Exception as e:
        logger.error(f"Failed to fetch todos: {str(e)}")
        return f"An error occurred: {str(e)}", 500

# Add new todo
@app.route("/add", methods=["POST"])
def add():
    task = request.form.get("task")
    if task:
        try:
            # Send to Kafka instead of directly to DB
            send_to_kafka(action="create", task=task, done=False)
        except Exception as e:
            logger.error(f"Failed to send task to Kafka: {str(e)}")
            return f"An error occurred: {str(e)}", 500
    return redirect(url_for("index"))

# Toggle todo done/undone
@app.route("/toggle/<int:id>")
def toggle(id):
    try:
        # Send to Kafka instead of directly to DB
        send_to_kafka(action="toggle", task_id=id)
    except Exception as e:
        logger.error(f"Failed to send toggle to Kafka: {str(e)}")
        return f"An error occurred: {str(e)}", 500
    return redirect(url_for("index"))

# Delete todo
@app.route("/delete/<int:id>")
def delete(id):
    try:
        # Send to Kafka instead of directly to DB
        send_to_kafka(action="delete", task_id=id)
    except Exception as e:
        logger.error(f"Failed to send delete to Kafka: {str(e)}")
        return f"An error occurred: {str(e)}", 500
    return redirect(url_for("index"))

if __name__ == "__main__":
    init_db()
    app.run(host="0.0.0.0", port=8080, debug=True)