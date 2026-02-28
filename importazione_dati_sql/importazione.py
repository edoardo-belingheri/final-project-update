from google.cloud import bigquery
import pandas as pd
import os

client = bigquery.Client()

# Cartella di output
OUTPUT_DIR = "data/initial_data"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Dizionario con tutte le tue query
QUERIES = {

    "distribution_centers": """
        SELECT id, name, latitude, longitude
        FROM bigquery-public-data.thelook_ecommerce.distribution_centers;
    """,

    "events": """
        SELECT a.*, b.event_type, b.traffic_source, b.state, b.created_at
        FROM(
            SELECT session_id, user_id, MAX(sequence_number) AS finish
            FROM bigquery-public-data.thelook_ecommerce.events
            GROUP BY session_id, user_id
        ) AS a
        JOIN bigquery-public-data.thelook_ecommerce.events AS b
        ON a.session_id=b.session_id AND a.finish=b.sequence_number;
    """,

    "inventory_items": """
        SELECT id, product_id, created_at, sold_at, product_distribution_center_id 
        FROM bigquery-public-data.thelook_ecommerce.inventory_items;
    """,

    "order_items": """
        SELECT *
        FROM bigquery-public-data.thelook_ecommerce.order_items;
    """,

    "orders": """
        SELECT order_id, user_id, status, created_at, returned_at, shipped_at, delivered_at, num_of_item
        FROM bigquery-public-data.thelook_ecommerce.orders;
    """,

    "products": """
        SELECT *
        FROM bigquery-public-data.thelook_ecommerce.products;
    """,

    "users": """
        SELECT id, first_name, last_name, age, gender, state, city, country, traffic_source
        FROM bigquery-public-data.thelook_ecommerce.users;
    """
}

# Esecuzione di tutte le query
for name, query in QUERIES.items():
    print(f"Eseguo query: {name}...")
    df = client.query(query).to_dataframe()
    output_path = f"{OUTPUT_DIR}/{name}.csv"
    df.to_csv(output_path, index=False)
    print(f"Salvato: {output_path}")

print("Estrazione completata!")
