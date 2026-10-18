#!/bin/sh
set -eu
# Existing role passwords are preserved. No data volume is recreated or reset.
if [ -n "${PGHOST:-}" ]; then
  PGPASSWORD=$(cat /run/secrets/postgres_password)
  export PGPASSWORD
fi
HORIZON_MIGRATION_PASSWORD=$(cat /run/secrets/migration_password)
HORIZON_WRITER_PASSWORD=$(cat /run/secrets/writer_password)
HORIZON_READER_PASSWORD=$(cat /run/secrets/reader_password)
export HORIZON_MIGRATION_PASSWORD HORIZON_WRITER_PASSWORD HORIZON_READER_PASSWORD
# psql reads values from its private environment instead of exposing them in argv.
psql --username "${POSTGRES_USER:-horizon_admin}" --dbname "${POSTGRES_DB:-horizon}" --set ON_ERROR_STOP=1 <<'SQL'
\getenv migration_pw HORIZON_MIGRATION_PASSWORD
\getenv writer_pw HORIZON_WRITER_PASSWORD
\getenv reader_pw HORIZON_READER_PASSWORD
SELECT format('CREATE ROLE horizon_migrator LOGIN PASSWORD %L', :'migration_pw')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='horizon_migrator') \gexec
SELECT format('CREATE ROLE horizon_writer LOGIN PASSWORD %L', :'writer_pw')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='horizon_writer') \gexec
SELECT format('CREATE ROLE horizon_reader LOGIN PASSWORD %L', :'reader_pw')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='horizon_reader') \gexec
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
GRANT USAGE, CREATE ON SCHEMA public TO horizon_migrator;
GRANT USAGE ON SCHEMA public TO horizon_writer,horizon_reader;
ALTER DEFAULT PRIVILEGES FOR ROLE horizon_migrator IN SCHEMA public
  GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO horizon_writer;
ALTER DEFAULT PRIVILEGES FOR ROLE horizon_migrator IN SCHEMA public
  GRANT USAGE, SELECT ON SEQUENCES TO horizon_writer;
ALTER DEFAULT PRIVILEGES FOR ROLE horizon_migrator IN SCHEMA public
  GRANT SELECT ON TABLES TO horizon_reader;
SQL
if [ "${APP_ENV:-development}" != "production" ]; then
  psql --username "${POSTGRES_USER:-horizon_admin}" --dbname "${POSTGRES_DB:-horizon}" --set ON_ERROR_STOP=1 \
    --command "SELECT 'CREATE DATABASE horizon_test OWNER horizon_migrator' WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname='horizon_test')" \
    --tuples-only --no-align | psql --username "${POSTGRES_USER:-horizon_admin}" --dbname "${POSTGRES_DB:-horizon}" --set ON_ERROR_STOP=1
fi
