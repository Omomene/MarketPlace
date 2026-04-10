from flask import Flask, request, jsonify
import random
from datetime import datetime

app = Flask(__name__)

# -------------------
# MOCK DATA (MVP ONLY)
# -------------------
SELLERS = [
    {"seller_id": "S1", "name": "Alice", "country": "FR"},
    {"seller_id": "S2", "name": "Bob", "country": "DE"},
]

PRODUCTS = [
    {"product_id": "P1", "name": "Shoes", "category": "Fashion", "base_price": 50},
    {"product_id": "P2", "name": "Bag", "category": "Fashion", "base_price": 80},
]

CUSTOMERS = [
    {"customer_id": "C1", "email": "a@test.com", "city": "Paris"},
    {"customer_id": "C2", "email": "b@test.com", "city": "Berlin"},
]

# -------------------
# ENDPOINTS
# -------------------

@app.get("/sellers")
def get_sellers():
    return jsonify(SELLERS)

@app.get("/products")
def get_products():
    return jsonify(PRODUCTS)

@app.get("/orders")
def get_orders():
    date = request.args.get("date", datetime.today().strftime("%Y-%m-%d"))

    orders = []
    for i in range(10):  
        orders.append({
            "order_id": f"O{i}",
            "seller_id": random.choice(SELLERS)["seller_id"],
            "customer_id": random.choice(CUSTOMERS)["customer_id"],
            "product_id": random.choice(PRODUCTS)["product_id"],
            "quantity": random.randint(1, 5),
            "total_amount": random.randint(20, 200),
            "status": "completed",
            "dt": date
        })

    return jsonify(orders)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)