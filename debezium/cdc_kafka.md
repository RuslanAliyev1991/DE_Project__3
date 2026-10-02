# Lesson 3 — CDC (Change Data Capture)

> **Architecture:**
> ```
> postgres-source (5432)
>       ↓ WAL
> Debezium (kafka-connect:8083)
>       ↓ CDC events
> Kafka topic: cdc.public.orders
>       ↓                    ↓
> cdc_event_log.py     cdc_db_sync.py
> (terminal log)       (postgres-target sync)
>                            ↓
>                   postgres-target (5433)
> ```


# Check status
docker compose -f docker-compose.cdc.yml ps
```

### 4. Verify Kafka Connect is ready (~30 seconds)

```bash
curl -s http://localhost:8083/ | python3 -m json.tool
```


### 8. Register connector

```bash
curl -s -X POST http://localhost:8084/connectors -H "Content-Type: application/json" -d @./debezium_connector/orders-cdc.json | python3 -m json.tool
```

## 1. List topics
```
./bin/kafka-topics.sh --bootstrap-server localhost:8050 --list
```

### 9. Check status — should be RUNNING

```bash
curl -s http://localhost:8084/connectors/orders-cdc-connector/status | python3 -m json.tool
```


### 11. Read first CDC messages

```bash
./bin/kafka-console-consumer.sh --bootstrap-server localhost:8050 --topic cdc.public.my_test --from-beginning --property print.key=true

```

**Expected — snapshot messages (`"op":"r"`):**
```json
{"before":null,"after":{"id":1,"customer_id":"cust-001","item":"Laptop","amount":"Aknw","status":"pending"},"op":"r"}
```

> The `amount` field comes as base64 encoded — we will decode it in the Python consumer.

---

## Op codes

| op | Meaning | When |
|----|---------|------|
| `r` | read (snapshot) | When connector starts for the first time |
| `c` | create (INSERT) | New row inserted |
| `u` | update (UPDATE) | Row updated |
| `d` | delete (DELETE) | Row deleted |



### 13. Run

```bash
python3 cdc_event_log.py
```

---

## B5 — Python Consumer 2: DB Sync

### 14. cdc_db_sync.py

```python
import json
import base64
import decimal
import psycopg2
from confluent_kafka import Consumer

kafka_conf = {
    'bootstrap.servers': '172.16.8.132:19092,172.16.8.132:29092,172.16.8.132:39092',
    'group.id': 'cdc-db-sync',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False,
}

pg = psycopg2.connect(
    host='172.16.8.132', port=5433,
    dbname='targetdb', user='postgres', password='postgres',
)
pg.autocommit = False

def decode_amount(value):
    if value is None:
        return None
    try:
        raw = base64.b64decode(value)
        unscaled = int.from_bytes(raw, byteorder='big', signed=True)
        return float(decimal.Decimal(unscaled) / 100)
    except Exception:
        return value

def decode_ts(ts_us):
    if ts_us is None:
        return None
    from datetime import datetime, timezone
    return datetime.fromtimestamp(ts_us / 1_000_000, tz=timezone.utc).replace(tzinfo=None)

def decode_row(row):
    return {
        'id':          row.get('id'),
        'customer_id': row.get('customer_id'),
        'item':        row.get('item'),
        'amount':      decode_amount(row.get('amount')),
        'status':      row.get('status'),
        'created_at':  decode_ts(row.get('created_at')),
        'updated_at':  decode_ts(row.get('updated_at')),
    }

def upsert(cur, row, op):
    r = decode_row(row)
    cur.execute("""
        INSERT INTO orders
            (id, customer_id, item, amount, status, created_at, updated_at, cdc_operation, synced_at)
        VALUES
            (%(id)s, %(customer_id)s, %(item)s, %(amount)s, %(status)s,
             %(created_at)s, %(updated_at)s, %(op)s, NOW())
        ON CONFLICT (id) DO UPDATE SET
            customer_id   = EXCLUDED.customer_id,
            item          = EXCLUDED.item,
            amount        = EXCLUDED.amount,
            status        = EXCLUDED.status,
            created_at    = EXCLUDED.created_at,
            updated_at    = EXCLUDED.updated_at,
            cdc_operation = EXCLUDED.cdc_operation,
            synced_at     = NOW()
    """, {**r, 'op': op})

def soft_delete(cur, row):
    """Soft delete — keep the row, set status='deleted'"""
    cur.execute("""
        UPDATE orders
        SET status='deleted', cdc_operation='DELETE', synced_at=NOW()
        WHERE id = %s
    """, (row['id'],))

def handle(op, before, after, cur):
    if op == 'r':
        upsert(cur, after, 'INSERT')
        print(f'  📸 SNAPSHOT  id={after["id"]}')
    elif op == 'c':
        upsert(cur, after, 'INSERT')
        print(f'  🟢 INSERT    id={after["id"]} | {after["item"]}')
    elif op == 'u':
        upsert(cur, after, 'UPDATE')
        print(f'  🔄 UPDATE    id={after["id"]} | status: {before.get("status")} → {after.get("status")}')
    elif op == 'd':
        soft_delete(cur, before)
        print(f'  🔴 DELETE    id={before["id"]} → soft deleted')

c = Consumer(kafka_conf)
c.subscribe(['cdc.public.orders'])
print('CDC DB Sync started...\n')

try:
    while True:
        msg = c.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            print(f'Kafka error: {msg.error()}')
            continue
        if msg.value() is None:          # tombstone
            c.commit(message=msg, asynchronous=False)
            continue

        payload = json.loads(msg.value().decode())
        op      = payload.get('op')
        before  = payload.get('before') or {}
        after   = payload.get('after')  or {}

        try:
            cur = pg.cursor()
            handle(op, before, after, cur)
            pg.commit()
            c.commit(message=msg, asynchronous=False)
        except Exception as e:
            pg.rollback()
            print(f'  ❌ Error: {e}')

except KeyboardInterrupt:
    print('\nStopped')
finally:
    pg.close()
    c.close()
```

### 15. Run

```bash
python3 cdc_db_sync.py
```

---

## B6 — Full Pipeline Test

**Open 3 terminals:**

```bash
# Terminal 1
python3 cdc_event_log.py

# Terminal 2
python3 cdc_db_sync.py

# Terminal 3 — make changes in source
docker exec -it postgres-source psql -U postgres -d sourcedb
```

```sql
-- INSERT
INSERT INTO orders (customer_id, item, amount, status)
VALUES ('cust-010', 'Tablet', 299.00, 'pending');

-- UPDATE
UPDATE orders SET status = 'shipped' WHERE id = 1;

-- DELETE (soft delete test)
DELETE FROM orders WHERE id = 2;
```

**Verify target:**

```bash
docker exec -it postgres-target psql -U postgres -d targetdb -c \
  "SELECT id, customer_id, item, amount, status, cdc_operation, created_at FROM orders ORDER BY id;"
```

**Expected result:**
```
 id | customer_id |   item   | amount  |  status   | cdc_operation |      created_at
----+-------------+----------+---------+-----------+---------------+---------------------
  1 | cust-001    | Laptop   | 1500.00 | shipped   | UPDATE        | 2026-04-13 21:21:14
  2 | cust-002    | Monitor  |  450.00 | deleted   | DELETE        | 2026-04-13 21:21:14
  3 | cust-001    | Keyboard |   85.00 | completed | INSERT        | 2026-04-13 21:21:14
  4 | cust-010    | Tablet   |  299.00 | pending   | INSERT        | 2026-04-13 21:23:45
```

---

## Useful Commands

```bash
# Delete and recreate connector
curl -X DELETE http://localhost:8083/connectors/orders-cdc-connector
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @./connectors/orders-cdc.json

# Verify WAL level
docker exec postgres-source psql -U postgres \
  -c "SHOW wal_level;"
# Must be: logical

# Reset consumer group offset
.bin/kafka-consumer-groups.sh --bootstrap-server localhost:8050 --group cdc-db-sync --topic cdc.public.my_test --reset-offsets --to-earliest --execute

# Truncate target table
docker exec -it postgres-target psql -U postgres -d targetdb -c "TRUNCATE orders;"

# View Connect logs
docker logs kafka-connect --tail 50
```

---

## Key Notes

| Point | Why it matters |
|-------|---------------|
| `REPLICA IDENTITY FULL` | Captures `before` values on UPDATE |
| `snapshot.mode=initial` | Reads existing data when connector starts |
| `amount` comes as base64 | Use `decode_amount()` to convert to float |
| Tombstone message | After DELETE, Debezium sends `value=None` — skip it |
| Soft delete | Keep rows in target, set `status='deleted'` |
| `kafka_default` network | CDC services connect to existing Kafka cluster |