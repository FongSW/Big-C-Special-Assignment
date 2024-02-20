DROP TABLE IF EXISTS phone_price_analysis;
CREATE TABLE phone_price_analysis (
    id SERIAL PRIMARY KEY,
    phone_brand VARCHAR(255),
    phone_model VARCHAR(255),
    phone_size VARCHAR(15),
    web_shop VARCHAR(50),
    title TEXT,
    name_store VARCHAR(255),
    rating_star FLOAT,
    path_source VARCHAR(255),
    path_search VARCHAR(255),
    price FLOAT,
    snapshot TIMESTAMP
);