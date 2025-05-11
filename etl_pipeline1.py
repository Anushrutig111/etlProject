import os
import gzip
import logging
import requests
import pandas as pd
from sqlalchemy import create_engine
import subprocess

# Setup Logging
logging.basicConfig(
    filename='etl_pipeline.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Constants
CSV_URL = "https://tyroo-engineering-assesments.s3.us-west-2.amazonaws.com/Tyroo-dummy-data.csv.gz"
CSV_PATH = "Tyroo-dummy-data.csv.gz"
DB_URI = "sqlite:///products.db"
CHUNK_SIZE = 50000

def fast_download(url, dest_path):
        logging.info("Starting file download by fast_download...")
        response = requests.get(url)
        logging.info("response genrated" + response)
        subprocess.run(["aria2c", "-x", "16", "-s", "16", "-o", dest_path, url])
        logging.info("subprocess ended")

# Download the file
def download_file(url, dest_path):
    try:
        logging.info("Starting file download...")
        response = requests.get(url)
        response.raise_for_status()
        with open(dest_path, 'wb') as f:
            f.write(response.content)
        logging.info("File downloaded successfully.")
    except Exception as e:
        logging.error(f"Error downloading file: {e}")
        raise

# Clean and transform chunk
def clean_chunk(chunk):
    # Fill NaNs
    chunk.fillna("", inplace=True)
    return chunk

# Save to SQL in chunk
def save_chunk_to_sql(chunk, engine):
    try:
        # Products
        chunk[['product_id', 'sku_id', 'product_name', 'description', 'product_url', 'deeplink', 'availability', 'brand_name']].to_sql('products', engine, if_exists='append', index=False)

        # Pricing & Commission
        chunk[['product_id', 'price', 'current_price', 'promotion_price', 'discount_percentage', 'platform_commission_rate', 'product_commission_rate', 'bonus_commission_rate']].to_sql('pricing_commission', engine, if_exists='append', index=False)

        # Images
        chunk[['product_id', 'product_small_img', 'product_medium_img', 'product_big_img', 'image_url_2', 'image_url_3', 'image_url_4', 'image_url_5']].to_sql('images', engine, if_exists='append', index=False)

        # Ratings
        chunk[['product_id', 'number_of_reviews', 'rating_avg_value']].to_sql('reviews_ratings', engine, if_exists='append', index=False)

        # Categories
        chunk[['product_id', 'venture_category1_name_en', 'venture_category2_name_en', 'venture_category3_name_en', 'venture_category_name_local']].to_sql('categories', engine, if_exists='append', index=False)

        # Sellers
        chunk[['seller_name', 'seller_url', 'seller_rating', 'business_type', 'business_area']].drop_duplicates().to_sql('sellers', engine, if_exists='append', index=False)
        
    except Exception as e:
        logging.error(f"Error saving chunk to database: {e}")
        raise

# Main ETL function
def run_etl():
    try:
        fast_download(CSV_URL, CSV_PATH)
        engine = create_engine(DB_URI)

        with gzip.open(CSV_PATH, 'rt', encoding='utf-8') as f:
            for i, chunk in enumerate(pd.read_csv(f, chunksize=CHUNK_SIZE, low_memory=False)):
                logging.info(f"Processing chunk {i}")
                cleaned = clean_chunk(chunk)
                save_chunk_to_sql(cleaned, engine)
                logging.info(f"Chunk {i} processed successfully.")
        logging.info("ETL process completed.")
    except Exception as e:
        logging.error(f"ETL process failed: {e}")
        raise

if __name__ == '__main__':
    run_etl()
