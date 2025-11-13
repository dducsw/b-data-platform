-- Create role if not exists
DO
$do$
BEGIN
   IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'gravitino') THEN
      CREATE ROLE gravitino WITH LOGIN PASSWORD 'gravitino123';
   END IF;
END
$do$;

-- Create schemas
CREATE SCHEMA IF NOT EXISTS gravitino_metadata;

-- Grant privileges
GRANT ALL PRIVILEGES ON SCHEMA gravitino_metadata TO gravitino;
GRANT ALL PRIVILEGES ON SCHEMA public TO gravitino;

-- Set default privileges for future tables
ALTER DEFAULT PRIVILEGES IN SCHEMA gravitino_metadata GRANT ALL ON TABLES TO gravitino;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO gravitino;

-- Create extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Log successful initialization
SELECT 'Gravitino PostgreSQL database initialized successfully' as status;
