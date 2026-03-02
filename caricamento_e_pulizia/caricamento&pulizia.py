import pandas as pd
import numpy as np
import logging
import io
import os
from datetime import datetime, timezone
from pathlib import Path

# --- CONFIGURAZIONE ---
BASE_PATH = Path(r"C:\Users\Dell\Documents\Progetti\aggiornamento_prog_finale\data")
INPUT_DIR = BASE_PATH / "initial_data"
OUTPUT_DIR = BASE_PATH / "clean_data"
LOG_DIR = BASE_PATH / "logs"

# Creazione directory se non esistono
OUTPUT_DIR.mkdir(exist_ok=True)
LOG_DIR.mkdir(exist_ok=True)

# Configurazione Logging
log_filename = LOG_DIR / f"pipeline_{datetime.now().strftime('%Y%m%d_%H%M')}.log"
logging.basicConfig(
    filename=log_filename,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    encoding='utf-8'
)

# --- FUNZIONI DI SUPPORTO ---
def log_df_info(df, name):
    """Cattura l'output di info() e lo scrive nel log."""
    buffer = io.StringIO()
    df.info(buf=buffer)
    info_str = buffer.getvalue()
    logging.info(f"--- Info per {name} ---\n{info_str}")

def clean_date_cols(df, columns):
    """Pulisce e normalizza le date."""
    for col in columns:
        if col in df.columns:
            df[col] = df[col].astype(str).str.replace(r"\.\d+", "", regex=True)
            df[col] = pd.to_datetime(df[col], utc=True)
    return df

# --- FUNZIONI DI PULIZIA ---
def process_inventory(df):
    df = clean_date_cols(df, ["created_at", "sold_at"])
    df['in_stock'] = df['sold_at'].isna().astype(int)
    df['month_year_creation'] = df['created_at'].dt.strftime('%b %Y')
    oggi = datetime.now(timezone.utc)
    df["sold_at"] = df["sold_at"].fillna(oggi)
    df["days_in_stock"] = (df["sold_at"] - df["created_at"]).dt.days
    return df[["id", "product_id", "product_distribution_center_id", "in_stock", "month_year_creation", "days_in_stock"]]

def process_products(df):
    df[["brand", "name"]] = df[["brand", "name"]].fillna("Unknown")
    df["cost"] = df["cost"].round(2)
    df["retail_price"] = df["retail_price"].round(2)
    return df[["id", "cost", "category", "name", "brand", "retail_price", "department", "distribution_center_id"]]

def process_users(df):
    df["city"] = df["city"].fillna("Unknown")
    bins = [0, 25, 45, 65, np.inf] 
    labels = ["Young", "Adult", "Middle-aged Adult", "Senior"] 
    df["age_group"] = pd.cut(df["age"], bins=bins, labels=labels, right=False)
    return df

def process_events(df, users_df):
    df = clean_date_cols(df, ["created_at"])
    df["year"], df["month"] = df["created_at"].dt.year, df["created_at"].dt.month
    city_country_map = users_df[['state', 'country']].drop_duplicates()
    df = df.merge(city_country_map, on='state', how='left').fillna("Unknown")
    return df[["session_id", "user_id", "finish", "event_type", "traffic_source", "state", "year", "month", "country"]]

def process_order_items(df, prod_df):
    df = clean_date_cols(df, ["created_at", "shipped_at", "delivered_at", "returned_at"])
    df["sale_price"] = df["sale_price"].round(2)
    df = df.merge(prod_df[["id", "cost"]], left_on="product_id", right_on="id", how="left")
    df.rename(columns={"id_x": "id"}, inplace=True)
    df["profit_per_product"] = (df["sale_price"] - df["cost"]).round(2)
    return df[["id", "order_id", "user_id", "product_id", "inventory_item_id", "status", "sale_price", "cost", "profit_per_product"]]

def process_orders(df, order_items_df):
    df = clean_date_cols(df, ["created_at", "shipped_at", "delivered_at", "returned_at"])
    df['month_year_creation'] = df['created_at'].dt.strftime('%b %Y')
    df["days_for_shipping"] = (df["shipped_at"] - df["created_at"]).dt.days
    df["days_for_delivery"] = (df["delivered_at"] - df["shipped_at"]).dt.days
    df["year"], df["month"] = df["created_at"].dt.year, df["created_at"].dt.month
    profits = order_items_df.groupby("order_id").agg({"profit_per_product": "sum"}).rename(columns={"profit_per_product": "profit"})
    df = df.merge(profits, on="order_id", how="left")
    df.loc[df['status'].isin(['Cancelled', 'Returned']), 'profit'] = 0
    return df[["order_id", "user_id", "status", "num_of_item", "month_year_creation", 
               "days_for_shipping", "days_for_delivery", "year", "month", "profit"]]

# --- ESECUZIONE ---
if __name__ == "__main__":
    logging.info("🚀 Inizio pipeline di pulizia dati...")
    
    # Caricamento
    raw = {name: pd.read_csv(INPUT_DIR / f"{name}.csv") for name in 
           ["distribution_centers", "events", "inventory_items", "order_items", "orders", "products", "Users"]}
    
    # Processamento
    clean_prod = process_products(raw["products"])
    clean_users = process_users(raw["Users"])
    
    cleaned_data = {
        "distribution_centers": raw["distribution_centers"],
        "inventory_items": process_inventory(raw["inventory_items"]),
        "products": clean_prod,
        "users": clean_users,
        "events": process_events(raw["events"], clean_users),
        "order_items": process_order_items(raw["order_items"], clean_prod),
        "orders": process_orders(raw["orders"], process_order_items(raw["order_items"], clean_prod))
    }
    
    # Salvataggio e Logging Info
    for name, df in cleaned_data.items():
        filename = f"{name}_clean.csv"
        df.to_csv(OUTPUT_DIR / filename, index=False)
        log_df_info(df, name) # Qui vedi le info nel log
        logging.info(f"💾 Salvato: {filename}")
        
    logging.info("✨ Pipeline completata con successo! 🎉")