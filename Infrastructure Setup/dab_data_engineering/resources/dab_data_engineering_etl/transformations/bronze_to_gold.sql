-- Create a materialized view for the date dimension from CSV files
CREATE MATERIALIZED VIEW sunny_bay_roastery.gold.dim_date AS
SELECT
    *
FROM read_files(
  "/Volumes/sunny_bay_roastery/bronze/raw/dim_date/",
  format => "csv"
);

-- Create a materialized view for the store dimension from CSV files
CREATE MATERIALIZED VIEW sunny_bay_roastery.gold.dim_store AS
SELECT
    *
FROM read_files(
  "/Volumes/sunny_bay_roastery/bronze/raw/dim_store/",
  format => "csv"
);

-- Create a materialized view for the customer dimension from CSV files
CREATE MATERIALIZED VIEW sunny_bay_roastery.gold.dim_customer AS
SELECT
    *
FROM read_files(
  "/Volumes/sunny_bay_roastery/bronze/raw/dim_customer/",
  format => "csv"
);

-- Create a materialized view for the product dimension from CSV files
CREATE MATERIALIZED VIEW sunny_bay_roastery.gold.dim_product AS
SELECT
    *
FROM read_files(
  "/Volumes/sunny_bay_roastery/bronze/raw/dim_product/",
  format => "csv"
);

-- Create a materialized view for the coffee sales fact table from Parquet files
CREATE MATERIALIZED VIEW sunny_bay_roastery.gold.fact_coffee_sales AS
SELECT
    *
FROM read_files(
  "/Volumes/sunny_bay_roastery/bronze/raw/fact_coffee_sales/",
  format => "parquet"
);