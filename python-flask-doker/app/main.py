
from flask import Flask, request, jsonify

app = Flask(__name__)

products = [
    {"id": 1, "name": "Laptop", "price": 75000, "stock": 5},
    {"id": 2, "name": "Mouse", "price": 1200, "stock": 20},
    {"id": 3, "name": "Keyboard", "price": 2500, "stock": 10},
]


@app.route("/")
def home():
    return jsonify({
        "message": "Product Management API",
        "endpoints": [
            "GET /products",
            "GET /products/<id>",
            "POST /products",
            "PUT /products/<id>",
            "DELETE /products/<id>"
        ]
    })


# Get all products + search
@app.route("/products", methods=["GET"])
def get_products():
    search = request.args.get("search", "").lower()

    result = [
        product for product in products
        if search in product["name"].lower()
    ]

    return jsonify({
        "success": True,
        "total": len(result),
        "data": result
    })


# Get a single product
@app.route("/products/<int:product_id>", methods=["GET"])
def get_product(product_id):
    product = next(
        (p for p in products if p["id"] == product_id),
        None
    )

    if product is None:
        return jsonify({"error": "Product not found"}), 404

    return jsonify(product)


# Create product
@app.route("/products", methods=["POST"])
def create_product():
    data = request.get_json(silent=True) or {}

    name = data.get("name")
    price = data.get("price")
    stock = data.get("stock", 0)

    if not isinstance(name, str) or not name.strip():
        return jsonify({"error": "Valid name is required"}), 400

    if (
        isinstance(price, bool)
        or not isinstance(price, (int, float))
        or price <= 0
    ):
        return jsonify({"error": "Price must be positive"}), 400

    if (
        isinstance(stock, bool)
        or not isinstance(stock, int)
        or stock < 0
    ):
        return jsonify({"error": "Stock must be a non-negative integer"}), 400

    new_id = max((p["id"] for p in products), default=0) + 1

    product = {
        "id": new_id,
        "name": name.strip(),
        "price": price,
        "stock": stock
    }

    products.append(product)

    return jsonify({
        "message": "Product created successfully",
        "data": product
    }), 201


# Update product
@app.route("/products/<int:product_id>", methods=["PUT"])
def update_product(product_id):
    data = request.get_json(silent=True) or {}

    product = next(
        (p for p in products if p["id"] == product_id),
        None
    )

    if product is None:
        return jsonify({"error": "Product not found"}), 404

    if "name" in data:
        if not isinstance(data["name"], str) or not data["name"].strip():
            return jsonify({"error": "Invalid name"}), 400
        product["name"] = data["name"].strip()

    if "price" in data:
        price = data["price"]
        if (
            isinstance(price, bool)
            or not isinstance(price, (int, float))
            or price <= 0
        ):
            return jsonify({"error": "Invalid price"}), 400
        product["price"] = price

    if "stock" in data:
        stock = data["stock"]
        if isinstance(stock, bool) or not isinstance(stock, int) or stock < 0:
            return jsonify({"error": "Invalid stock"}), 400
        product["stock"] = stock

    return jsonify({
        "message": "Product updated successfully",
        "data": product
    })


# Delete product
@app.route("/products/<int:product_id>", methods=["DELETE"])
def delete_product(product_id):
    product = next(
        (p for p in products if p["id"] == product_id),
        None
    )

    if product is None:
        return jsonify({"error": "Product not found"}), 404

    products.remove(product)

    return jsonify({
        "message": "Product deleted successfully"
    })


if __name__ == "__main__":
    app.run(debug=True, port=5000)



    
        from collections import OrderedDict
from threading import RLock
from typing import Generic, TypeVar

K = TypeVar("K")
V = TypeVar("V")


class LRUCache(Generic[K, V]):
    def __init__(self, capacity: int):
        if capacity <= 0:
            raise ValueError("Capacity must be positive")

        self.capacity = capacity
        self.cache: OrderedDict[K, V] = OrderedDict()
        self.lock = RLock()

    def get(self, key: K) -> V | None:
        # TODO: Return the value and mark it recently used
        raise NotImplementedError

    def put(self, key: K, value: V) -> None:
        # TODO: Insert/update and evict the least recently used item
        raise NotImplementedError

    def __len__(self) -> int:
        with self.lock:
            return len(self.cache)




            import json
from pathlib import Path
from threading import RLock
from typing import Any


class PersistentStore:
    def __init__(self, file_path: str):
        self.path = Path(file_path)
        self.lock = RLock()
        self.data: dict[str, Any] = {}

        # TODO: Load existing data from disk.

    def set(self, key: str, value: Any) -> None:
        # TODO: Update data and persist safely.
        raise NotImplementedError

    def get(self, key: str, default=None):
        # TODO: Return stored value.
        raise NotImplementedError

    def delete(self, key: str) -> bool:
        # TODO: Delete key and persist.
        raise NotImplementedError

    def transaction(self, updates: dict[str, Any]) -> None:
        # TODO: Apply multiple updates atomically.
        raise NotImplementedError



        