DROP TABLE IF EXISTS member_lf;
DROP TYPE IF EXISTS gender_enum;
CREATE TYPE gender_enum AS ENUM ('M', 'F', 'NA');
CREATE TABLE member_lf (
    member_id VARCHAR(8) PRIMARY KEY,
    sex gender_enum,
    year_of_birth YEAR,
    location TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS member_analysis;

CREATE TABLE member_analysis (
    id SERIAL PRIMARY KEY,
    member_id VARCHAR(8),
    recency DATE NOT NULL,
    frequency_1w  INT NOT NULL,
    monetary_value FlOAT NOT NULL,
    snapshot TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (member_id) REFERENCES member_lf(member_id)
);

DROP TABLE IF EXISTS bill_lf;

CREATE TABLE bill_lf (
    id SERIAL PRIMARY KEY,
    bill_no VARCHAR(8) NOT NULL,
    bill_timestamp TIMESTAMP NOT NULL,
    member_id VARCHAR(8),
    sku_code VARCHAR(7) NOT NULL,
    sku_name VARCHAR(150) NOT NULL,
    sku_category VARCHAR(40) NOT NULL,
    qty INT NOT NULL,
    sales FlOAT NOT NULL,
    snapshot TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (member_id) REFERENCES member_lf(member_id)
);
