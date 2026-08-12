# The one Kafka Connect worker for the whole pipeline — shared infra, not
# specific to one bounded context (same reasoning as kafka/create-topics.sh
# living at the top level rather than under catalog-input/), which is why
# this moved out of catalog-input/connect/ once it started hosting
# reporting-output's sink connector alongside catalog-input's source
# connector. Connector *configs* still live with the bounded context that
# owns them (catalog-input/connect/*.json, reporting-output/connect/*.json).
#
# The obvious base image is debezium/connect, but that image doesn't ship
# the Confluent Avro converter needed for Schema Registry integration.
# confluentinc/cp-kafka-connect-base ships confluent-hub, which
# lets us install the Debezium Postgres connector, the JDBC sink connector,
# and the Avro converter with three declarative lines — no custom code, just
# plugin installation.
#
# Debezium connector pinned to the 2.5.x line (not :latest / 3.x): this base
# image ships a Java 11 JRE, and Debezium 3.x connector jars are compiled for
# Java 17 (UnsupportedClassVersionError at plugin-scan time otherwise).
#
# kafka-connect-jdbc bundles the PostgreSQL driver directly (BSD-licensed,
# unlike the MySQL/Oracle/SQL Server drivers it deliberately omits) — no
# separate driver download needed for the reporting-output sink connectors.
# client-input's MySQL sink connectors need that driver added separately
# (MySQL Connector/J, GPL-licensed — why kafka-connect-jdbc won't bundle it
# itself): downloaded straight from Maven Central and dropped into the JDBC
# connector's own lib/ dir, which is already on CONNECT_PLUGIN_PATH.
#
# debezium-connector-mysql: no 2.5.x build exists on Confluent Hub for this
# one (available versions jump 2.4.2-1 -> 3.0.8) - 2.4.2-1 is the newest
# still on Java 11, for the same reason the Postgres connector above is
# pinned below 3.x.
FROM confluentinc/cp-kafka-connect-base:7.6.1

RUN confluent-hub install --no-prompt debezium/debezium-connector-postgresql:2.5.4-2 \
 && confluent-hub install --no-prompt debezium/debezium-connector-mysql:2.4.2-1 \
 && confluent-hub install --no-prompt confluentinc/kafka-connect-avro-converter:7.6.1 \
 && confluent-hub install --no-prompt confluentinc/kafka-connect-jdbc:10.7.4 \
 && curl -fsSL -o /usr/share/confluent-hub-components/confluentinc-kafka-connect-jdbc/lib/mysql-connector-j.jar \
      https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.4.0/mysql-connector-j-8.4.0.jar
