from flask import Flask, request, jsonify
import random
from datetime import datetime

app = Flask(__name__)

# -------------------------
# AUTH CONFIG (SIMPLE MVP)
# -------------------------
API_TOKEN = "formation-token-2026"

def require_bearer_auth():
    auth = request.headers.get("Authorization")

    if not auth or not auth.startswith("Bearer "):
        return False

    token = auth.split(" ")[1]
    return token == API_TOKEN


# -------------------------
# MOCK ENTITIES (IMPROVED)
# -------------------------

SELLERS = [
    {"seller_id": "S1", "name": "Maison Luxe", "country": "FR"},
    {"seller_id": "S2", "name": "Urban Style", "country": "DE"},
    {"seller_id": "S3", "name": "Tech World", "country": "US"},
    {"seller_id": "S4", "name": "Vintage Corner", "country": "IT"},
]

PRODUCTS = [
    {"product_id": "P1", "name": "Designer Shoes", "category": "Fashion", "base_price": 120},
    {"product_id": "P2", "name": "Luxury Bag", "category": "Fashion", "base_price": 250},
    {"product_id": "P3", "name": "Headphones", "category": "Electronics", "base_price": 80},
    {"product_id": "P4", "name": "Vintage Jacket", "category": "Fashion", "base_price": 150},
]

CUSTOMERS = [
    {"customer_id": "C1", "email": "alice@test.com", "city": "Paris"},
    {"customer_id": "C2", "email": "bob@test.com", "city": "Berlin"},
    {"customer_id": "C3", "email": "john@test.com", "city": "New York"},
]


# -------------------------
# HEALTH CHECK
# -------------------------
@app.get("/health")
def health():
    return {"status": "ok"}


# -------------------------
# SELLERS
# -------------------------
@app.get("/sellers")
def get_sellers():
    if not require_bearer_auth():
        return jsonify({"error": "Unauthorized"}), 401

    return jsonify(SELLERS)


# -------------------------
# PRODUCTS
# -------------------------
@app.get("/products")
def get_products():
    if not require_bearer_auth():
        return jsonify({"error": "Unauthorized"}), 401

    return jsonify(PRODUCTS)


# -------------------------
# ORDERS (ANOMALY READY)
# -------------------------
SELLER_MULTIPLIERS = {
    "S1": 1.5,
    "S2": 1.2,
    "S3": 1.0,
    "S4": 0.7,
}

@app.get("/orders")
def get_orders():
    if not require_bearer_auth():
        return jsonify({"error": "Unauthorized"}), 401

    date = request.args.get("date", datetime.today().strftime("%Y-%m-%d"))

    orders = []

    for i in range(20):

        seller = random.choice(SELLERS)
        product = random.choice(PRODUCTS)

        base = product["base_price"]
        quantity = random.randint(1, 5)

        # base revenue
        total = base * quantity

        # seller behavior
        multiplier = SELLER_MULTIPLIERS.get(seller["seller_id"], 1)

        # -------------------------
        # CONTROLLED ANOMALY ENGINE
        # -------------------------
        anomaly_factor = 1.0

        # 8% chance of drop anomaly
        if random.random() < 0.08:
            anomaly_factor = 0.4

        # 3% chance of spike anomaly
        if random.random() < 0.03:
            anomaly_factor = 2.0

        total_amount = total * multiplier * anomaly_factor

        orders.append({
            "order_id": f"O{date.replace('-', '')}{i}",
            "seller_id": seller["seller_id"],
            "customer_id": random.choice(CUSTOMERS)["customer_id"],
            "product_id": product["product_id"],
            "quantity": quantity,
            "total_amount": round(total_amount, 2),
            "status": "completed",
            "dt": date
        })

    return jsonify(orders)


# -------------------------
# RUN
# -------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
    