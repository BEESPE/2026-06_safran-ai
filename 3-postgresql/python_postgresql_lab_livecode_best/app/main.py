from flask import (
    Flask,
    render_template,
    request,
    redirect,
    url_for,
    session,
    flash,
)
import psycopg2
import os


app = Flask(__name__)
app.secret_key = "inventory_lab_secret_key"

USERNAME = "admin"
PASSWORD = "admin123"


@app.route("/login", methods=["GET", "POST"])
def login():
    error = None

    if request.method == "POST":
        username = request.form["username"]
        password = request.form["password"]

        if username == USERNAME and password == PASSWORD:
            session["logged_in"] = True
            return redirect(url_for("index"))
        else:
            error = "Invalid username or password"

    return render_template("login.html", error=error)


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


def get_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "postgres"),
        database=os.getenv("DB_NAME", "inventory_db"),
        user=os.getenv("DB_USER", "admin"),
        password=os.getenv("DB_PASSWORD", "admin123"),
        port=os.getenv("DB_PORT", "5432")
    )


def check_session():
    if not session.get("logged_in"):
        return redirect(url_for("login"))

@app.route("/")
def index():
    check_session()

    conn = get_connection()
    cur = conn.cursor()

    cur.execute("SELECT * FROM products ORDER BY id")
    products = cur.fetchall()

    cur.close()
    conn.close()

    return render_template("index.html", products=products)

@app.route("/add", methods=["POST"])
def add_product():
    check_session()

    name = request.form["name"]
    price = request.form["price"]
    stock = request.form["stock"]

    if stock and int(stock) < 0:
        flash("Stock cannot be negative.", "error")
        return redirect(url_for("index"))


    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        "INSERT INTO products(name, price, stock) VALUES(%s,%s,%s)",
        (name, price, stock)
    )

    conn.commit()
    cur.close()
    conn.close()

    return redirect(url_for("index"))

@app.route("/edit/<int:id>")
def edit_form(id):
    check_session()

    conn = get_connection()
    cur = conn.cursor()

    cur.execute("SELECT * FROM products WHERE id=%s", (id,))
    product = cur.fetchone()

    cur.close()
    conn.close()

    return render_template("edit.html", product=product)

@app.route("/update/<int:id>", methods=["POST"])
def update_product(id):
    check_session()

    name = request.form["name"]
    price = request.form["price"]
    stock = request.form["stock"]

    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        UPDATE products
        SET name=%s, price=%s, stock=%s
        WHERE id=%s
        """,
        (name, price, stock, id)
    )

    conn.commit()
    cur.close()
    conn.close()

    return redirect(url_for("index"))

@app.route("/delete/<int:id>")
def delete_product(id):
    check_session()

    conn = get_connection()
    cur = conn.cursor()

    cur.execute("DELETE FROM products WHERE id=%s", (id,))

    conn.commit()
    cur.close()
    conn.close()

    return redirect(url_for("index"))


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True, use_reloader=True)