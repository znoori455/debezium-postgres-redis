#!/bin/bash
set -e

echo "🚀 Setting up Debezium CDC Pipeline..."

# Wait for Debezium Connect to be ready
echo "⏳ Waiting for Debezium Connect to be ready..."
until curl -s http://localhost:8083/ > /dev/null; do
    echo "   Waiting for Debezium Connect..."
    sleep 3
done
echo "✓ Debezium Connect is ready"

# Register Postgres connector
echo "📝 Registering Postgres connector..."
curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" \
  http://localhost:8083/connectors/ -d '{
  "name": "postgres-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "postgres",
    "database.port": "5432",
    "database.user": "postgres",
    "database.password": "postgres",
    "database.dbname": "testdb",
    "database.server.name": "dbserver1",
    "table.include.list": "public.users",
    "plugin.name": "pgoutput",
    "publication.autocreate.mode": "filtered",
    "topic.prefix": "dbserver1",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "false",
    "value.converter.schemas.enable": "false"
  }
}'

echo ""
echo "✓ Connector registered!"
echo ""
echo "📊 Checking connector status..."
sleep 3
curl -s http://localhost:8083/connectors/postgres-connector/status | python3 -m json.tool

echo ""
echo "✅ Setup complete!"
echo ""
echo "The CDC pipeline is now active:"
echo "  Postgres → Debezium → Kafka → Consumer → Redis"
echo ""
echo "Run 'python3 test.py' to verify the pipeline"