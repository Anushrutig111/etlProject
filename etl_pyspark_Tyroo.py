#final code
import os
import gzip
import logging
import requests
import pandas as pd
from sqlalchemy import create_engine
import shutil

# Setup Logging
logging.basicConfig(
    filename='etl_pipeline.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Constants
CSV_URL = "https://tyroo-engineering-assesments.s3.us-west-2.amazonaws.com/Tyroo-dummy-data.csv.gz"
CSV_GZ_PATH = "Tyroo-dummy-data.csv.gz"
CSV_PATH = "Tyroo-dummy-data.csv"
DB_URI = "sqlite:///products.db"
CHUNK_SIZE = 50000


def download_file(url: str, output_path: str, chunk_size: int = 1024 * 1024) -> None:
    try:
        with requests.get(url, stream=True, timeout=30) as response:
            response.raise_for_status()
            with open(output_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=chunk_size):
                    if chunk:
                        f.write(chunk)
        logging.info(f"Download complete: {output_path}")
    except requests.exceptions.RequestException as e:
        logging.error(f"Download failed: {e}")
        raise


def decompress_gzip(source_path: str, output_path: str) -> None:
    if not os.path.exists(source_path):
        raise FileNotFoundError(f"Source file not found: {source_path}")

    try:
        with gzip.open(source_path, 'rb') as f_in:
            with open(output_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        logging.info(f"Decompression complete: {output_path}")
    except (OSError, gzip.BadGzipFile) as e:
        logging.error(f"Failed to decompress file: {e}")
        raise


def clean_chunk(chunk):
    chunk.fillna("", inplace=True)

    # Remove leading/trailing whitespaces from strings
    obj_cols = chunk.select_dtypes(include='object').columns
    for col in obj_cols:
        chunk[col] = chunk[col].astype(str).str.strip()

    # Convert numeric fields
    numeric_cols = ['price', 'current_price', 'promotion_price', 'discount_percentage',
                    'platform_commission_rate', 'product_commission_rate', 'bonus_commission_rate',
                    'rating_avg_value', 'number_of_reviews', 'seller_rating']
    for col in numeric_cols:
        if col in chunk.columns:
            chunk[col] = pd.to_numeric(chunk[col], errors='coerce')

    # Ensure unique ID where needed
    chunk.drop_duplicates(subset='product_id', inplace=True)

    return chunk


def save_chunk_to_sql(chunk, engine):
    try:
        chunk[['product_id', 'sku_id', 'product_name', 'description', 'product_url', 'deeplink', 'availability', 'brand_name']].to_sql('products', engine, if_exists='append', index=False)

        chunk[['product_id', 'price', 'current_price', 'promotion_price', 'discount_percentage',
               'platform_commission_rate', 'product_commission_rate', 'bonus_commission_rate']].to_sql('pricing_commission', engine, if_exists='append', index=False)

        chunk[['product_id', 'product_small_img', 'product_medium_img', 'product_big_img', 'image_url_2', 'image_url_3', 'image_url_4', 'image_url_5']].to_sql('images', engine, if_exists='append', index=False)

        chunk[['product_id', 'number_of_reviews', 'rating_avg_value']].to_sql('reviews_ratings', engine, if_exists='append', index=False)

        chunk[['product_id', 'venture_category1_name_en', 'venture_category2_name_en', 'venture_category3_name_en', 'venture_category_name_local']].to_sql('categories', engine, if_exists='append', index=False)

        chunk[['seller_name', 'seller_url', 'seller_rating', 'business_type', 'business_area']].drop_duplicates().to_sql('sellers', engine, if_exists='append', index=False)

    except Exception as e:
        logging.error(f"Error saving chunk to database: {e}")
        raise


def run_etl():
    try:
        logging.info("Starting ETL pipeline...")
        download_file(CSV_URL, CSV_GZ_PATH)
        decompress_gzip(CSV_GZ_PATH, CSV_PATH)

        engine = create_engine(DB_URI)

        with open(CSV_PATH, 'r', encoding='utf-8') as f:
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
