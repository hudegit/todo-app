from flask import Flask, render_template, request, redirect, url_for
import psycopg2
import os

app = Flask(__name__)

# Database connection
def get_db():
    return psycopg2.connect(
        host=os.environ.get("DB_HOST", "localhost"),
        database=os.environ.get("DB_NAME", "learningdb"),
        user=os.environ.get("DB_USER", "student"),
        password=os.environ.get("DB_PASSWORD", "password123"),
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
        conn = get_db()
        cur = conn.cursor()
        cur.execute("INSERT INTO todos (task) VALUES (%s)", (task,))
        conn.commit()
        cur.close()
        conn.close()
    return redirect(url_for("index"))

# Toggle todo done/undone
@app.route("/toggle/<int:id>")
def toggle(id):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("UPDATE todos SET done = NOT done WHERE id = %s", (id,))
    conn.commit()
    cur.close()
    conn.close()
    return redirect(url_for("index"))

# Delete todo
@app.route("/delete/<int:id>")
def delete(id):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("DELETE FROM todos WHERE id = %s", (id,))
    conn.commit()
    cur.close()
    conn.close()
    return redirect(url_for("index"))

if __name__ == "__main__":
    init_db()
    app.run(host="0.0.0.0", port=8080, debug=True)