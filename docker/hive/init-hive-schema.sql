-- Create database for Hive Metastore
CREATE DATABASE IF NOT EXISTS hive_metastore;

-- Create user for Hive
CREATE USER IF NOT EXISTS hive WITH PASSWORD 'hive123';

-- Grant privileges
GRANT ALL PRIVILEGES ON DATABASE hive_metastore TO hive;

-- Switch to hive_metastore database
\c hive_metastore;

-- Grant schema privileges
GRANT ALL ON SCHEMA public TO hive;