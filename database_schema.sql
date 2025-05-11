-- Products Table
CREATE TABLE IF NOT EXISTS products (
    product_id TEXT,
    sku_id TEXT,
    product_name TEXT,
    description TEXT,
    product_url TEXT,
    deeplink TEXT,
    availability TEXT,
    brand_name TEXT
);

-- Pricing and Commission Table
CREATE TABLE IF NOT EXISTS pricing_commission (
    product_id TEXT,
    price TEXT,
    current_price TEXT,
    promotion_price TEXT,
    discount_percentage TEXT,
    platform_commission_rate TEXT,
    product_commission_rate TEXT,
    bonus_commission_rate TEXT
);

-- Images Table
CREATE TABLE IF NOT EXISTS images (
    product_id TEXT,
    product_small_img TEXT,
    product_medium_img TEXT,
    product_big_img TEXT,
    image_url_2 TEXT,
    image_url_3 TEXT,
    image_url_4 TEXT,
    image_url_5 TEXT
);

-- Reviews and Ratings Table
CREATE TABLE IF NOT EXISTS reviews_ratings (
    product_id TEXT,
    number_of_reviews TEXT,
    rating_avg_value TEXT
);

-- Categories Table
CREATE TABLE IF NOT EXISTS categories (
    product_id TEXT,
    venture_category1_name_en TEXT,
    venture_category2_name_en TEXT,
    venture_category3_name_en TEXT,
    venture_category_name_local TEXT
);

-- Sellers Table
CREATE TABLE IF NOT EXISTS sellers (
    seller_name TEXT,
    seller_url TEXT,
    seller_rating TEXT,
    business_type TEXT,
    business_area TEXT
);
