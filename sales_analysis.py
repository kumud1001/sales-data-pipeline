import csv
from collections import defaultdict


FILE = "data/processed_sales-00000-of-00001.csv"


def load_data():
    with open(FILE, "r", encoding="utf-8") as file:
        return list(csv.DictReader(file))


def analyze_sales(data):

    total_revenue = sum(
        float(row["revenue"])
        for row in data
    )

    total_orders = len(data)

    average_order_value = (
        total_revenue / total_orders
        if total_orders > 0
        else 0
    )

    product_revenue = defaultdict(float)

    for row in data:
        product_revenue[row["product"]] += float(
            row["revenue"]
        )

    customer_revenue = defaultdict(float)

    for row in data:
        customer_revenue[row["customer"]] += float(
            row["revenue"]
        )

    print("\n===== SALES ANALYSIS =====")

    print(f"Total Orders: {total_orders}")
    print(f"Total Revenue: ${total_revenue:.2f}")
    print(f"Average Order Value: ${average_order_value:.2f}")

    print("\nRevenue by Product:")

    for product, revenue in product_revenue.items():
        print(f"{product}: ${revenue:.2f}")

    print("\nRevenue by Customer:")

    for customer, revenue in customer_revenue.items():
        print(f"{customer}: ${revenue:.2f}")


if __name__ == "__main__":

    data = load_data()

    analyze_sales(data)