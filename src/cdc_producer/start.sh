# 1. Create the table first (so the connector has something to watch)
python create_table.py

# 2. Wait a few seconds to ensure DB is ready/table is committed if needed
sleep 5

# 3. Register the connector
bash run.sh register_connector ./configs/postgresql-cdc.json