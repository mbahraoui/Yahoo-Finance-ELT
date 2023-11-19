-- Create the 'transformed' schema
CREATE SCHEMA IF NOT EXISTS transformed;

-- Create date_dimension table
CREATE TABLE IF NOT EXISTS transformed.date_dimension (
    date_key INT IDENTITY(1,1) PRIMARY KEY,
    calendar_date DATE,
    day INT,
    month INT,
    quarter INT,
    year INT
);

-- Create stock_dimension table
CREATE TABLE IF NOT EXISTS transformed.stock_dimension (
    stock_symbol VARCHAR(10) PRIMARY KEY,
    company_name VARCHAR(255),
    industry VARCHAR(255),
    sector VARCHAR(255)
);

-- Create market_index_dimension table
CREATE TABLE IF NOT EXISTS transformed.market_index_dimension (
    index_symbol VARCHAR(10) PRIMARY KEY,
    index_name VARCHAR(255),
    description VARCHAR(255)
);

-- Create stock_price_fact table
CREATE TABLE IF NOT EXISTS transformed.stock_price_fact (
    date_key INT REFERENCES transformed.date_dimension(date_key),
    stock_symbol VARCHAR(10) REFERENCES transformed.stock_dimension(stock_symbol),
    open_price DECIMAL(20, 10),
    close_price DECIMAL(20, 10),
    high_price DECIMAL(20, 10),
    low_price DECIMAL(20, 10),
    volume BIGINT,
    PRIMARY KEY (date_key, stock_symbol)
) DISTKEY(date_key) SORTKEY(date_key, stock_symbol);

-- Create market_index_fact table
CREATE TABLE IF NOT EXISTS transformed.market_index_fact (
    date_key INT REFERENCES transformed.date_dimension(date_key),
    index_symbol VARCHAR(10) REFERENCES transformed.market_index_dimension(index_symbol),
    open_price DECIMAL(20, 10),
    close_price DECIMAL(20, 10),
    high_price DECIMAL(20, 10),
    low_price DECIMAL(20, 10),
    volume BIGINT,
    PRIMARY KEY (date_key, index_symbol)
) DISTKEY(date_key) SORTKEY(date_key, index_symbol);





-- Transformations

-- Populate date_dimension from stock_data_staging and index_data_staging tables
INSERT INTO transformed.date_dimension (calendar_date, day, month, quarter, year)
SELECT DISTINCT stock_Date, EXTRACT(DAY FROM stock_Date), EXTRACT(MONTH FROM stock_Date),
               EXTRACT(QUARTER FROM stock_Date), EXTRACT(YEAR FROM stock_Date)
FROM stock_data_staging
UNION
SELECT DISTINCT index_Date, EXTRACT(DAY FROM index_Date), EXTRACT(MONTH FROM index_Date),
               EXTRACT(QUARTER FROM index_Date), EXTRACT(YEAR FROM index_Date)
FROM index_data_staging ;

-- Populate stock_dimension from company_info_staging table
INSERT INTO transformed.stock_dimension (stock_symbol, company_name, industry, sector)
SELECT DISTINCT Symbol, Name, Industry, Sector
FROM company_info_staging;

-- Populate market_index_dimension from indices_info_staging table
INSERT INTO transformed.market_index_dimension (index_symbol, index_name, description)
SELECT DISTINCT Symbol, Name, Description
FROM indices_info_staging;

-- Populate stock_price_fact from stock_data_staging table
INSERT INTO transformed.stock_price_fact (date_key, stock_symbol, open_price, close_price, high_price, low_price, volume)
SELECT d.date_key, s.Symbol, s."Open", s.Close, s.High, s.Low, s.Volume
FROM transformed.date_dimension d
JOIN stock_data_staging s ON d.calendar_date = s.stock_Date;

-- Populate market_index_fact from index_data_staging table
INSERT INTO transformed.market_index_fact (date_key, index_symbol, open_price, close_price, high_price, low_price, volume)
SELECT d.date_key, i.Symbol, i."Open", i.Close, i.High, i.Low, i.Volume
FROM transformed.date_dimension d
JOIN index_data_staging i ON d.calendar_date = i.index_Date;


-- stored procedure

-- Create the stored procedure
CREATE OR REPLACE PROCEDURE populate_transformed_data()
AS $$
BEGIN
    -- Populate date_dimension from stock_data_staging and index_data_staging tables
    INSERT INTO transformed.date_dimension (calendar_date, day, month, quarter, year)
    SELECT DISTINCT stock_Date, EXTRACT(DAY FROM stock_Date), EXTRACT(MONTH FROM stock_Date),
                   EXTRACT(QUARTER FROM stock_Date), EXTRACT(YEAR FROM stock_Date)
    FROM stock_data_staging
    UNION
    SELECT DISTINCT index_Date, EXTRACT(DAY FROM index_Date), EXTRACT(MONTH FROM index_Date),
                   EXTRACT(QUARTER FROM index_Date), EXTRACT(YEAR FROM index_Date)
    FROM index_data_staging;

    -- Populate stock_dimension from company_info_staging table
    INSERT INTO transformed.stock_dimension (stock_symbol, company_name, industry, sector)
    SELECT DISTINCT Symbol, Name, Industry, Sector
    FROM company_info_staging;

    -- Populate market_index_dimension from indices_info_staging table
    INSERT INTO transformed.market_index_dimension (index_symbol, index_name, description)
    SELECT DISTINCT Symbol, Name, Description
    FROM indices_info_staging;

    -- Populate stock_price_fact from stock_data_staging table
    INSERT INTO transformed.stock_price_fact (date_key, stock_symbol, open_price, close_price, high_price, low_price, volume)
    SELECT d.date_key, s.Symbol, s."Open", s.Close, s.High, s.Low, s.Volume
    FROM transformed.date_dimension d
    JOIN stock_data_staging s ON d.calendar_date = s.stock_Date;

    -- Populate market_index_fact from index_data_staging table
    INSERT INTO transformed.market_index_fact (date_key, index_symbol, open_price, close_price, high_price, low_price, volume)
    SELECT d.date_key, i.Symbol, i."Open", i.Close, i.High, i.Low, i.Volume
    FROM transformed.date_dimension d
    JOIN index_data_staging i ON d.calendar_date = i.index_Date;
END;
$$ LANGUAGE plpgsql;
