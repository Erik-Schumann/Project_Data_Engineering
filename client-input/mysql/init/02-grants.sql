-- MYSQL_USER/MYSQL_PASSWORD (env vars on the client-input-db service)
-- already created this user as part of container bootstrap, before any
-- docker-entrypoint-initdb.d script runs - this just extends it with what
-- that bootstrap doesn't grant on its own.
--
-- mysql_native_password, not MySQL 8's caching_sha2_password default:
-- avoids RSA-public-key-retrieval friction between PyMySQL (generator.py)
-- and a plaintext local connection - not a real security boundary in this
-- single-developer Docker Compose stack (see docker-compose.yml's TLS
-- note for the same tradeoff made elsewhere).
ALTER USER 'client_input'@'%' IDENTIFIED WITH mysql_native_password BY 'client_input';

-- Nothing else to grant: the container's own MYSQL_USER/MYSQL_DATABASE
-- bootstrap already gives this user full privileges on the client_input
-- database (items/users - the two JDBC sink connectors write, generator.py
-- reads). This file used to also grant REPLICATION SLAVE/CLIENT/RELOAD for
-- Debezium's MySqlConnector (client-input-source-connector, which CDC'd
-- ratings/watch_events out of a local outbox table) - removed along with
-- that connector, see ../../generator.py's module docstring and
-- ../../README.md's "How events actually reach Kafka now". Nothing here
-- reads the binlog any more.
FLUSH PRIVILEGES;
