import os, json, sys, time, traceback, psycopg2
from confluent_kafka import Consumer, KafkaException

BOOT = os.getenv("KAFKA_BOOTSTRAP","kafka:9092")
TOPIC = os.getenv("TOPIC","currency.raw")

PG = dict(
    host=os.getenv("PG_HOST","postgres"),
    dbname=os.getenv("PG_DB","raw"),
    user=os.getenv("PG_USER","postgres"),
    password=os.getenv("PG_PASSWORD","postgres")
)

print(f"[startup] connecting to PG: {PG}", flush=True)
conn = psycopg2.connect(**PG); conn.autocommit=True
cur = conn.cursor()

SQL = """
INSERT INTO raw_currency (base_currency, symbol, rate, fetched_at, kafka_offset, payload)
VALUES (%s,%s,%s,%s,%s,%s)
"""

# paksa baca dari awal dengan group id baru
group_id = os.getenv("KAFKA_GROUP_ID", "currency-consumer-2")
print(f"[startup] Kafka bootstrap={BOOT}, topic={TOPIC}, group_id={group_id}", flush=True)

c = Consumer({
    "bootstrap.servers": BOOT,
    "group.id": group_id,
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False
})
c.subscribe([TOPIC])

try:
    last_log = time.time()
    while True:
        msg = c.poll(1.0)
        if msg is None:
            if time.time() - last_log > 10:
                print("[heartbeat] waiting for messages...", flush=True)
                last_log = time.time()
            continue
        if msg.error():
            raise KafkaException(msg.error())
        try:
            payload = json.loads(msg.value())
            base = payload["base_currency"]
            sym  = payload["symbol"]
            rate = float(payload["rate"])
            ts   = payload["fetched_at"]
            cur.execute(SQL, (base, sym, rate, ts, msg.offset(), json.dumps(payload)))
            c.commit(asynchronous=False)
            print(f"[ok] offset={msg.offset()} key={sym} rate={rate} ts={ts}", flush=True)
        except Exception as e:
            print("[ERR] insert failed:", e, file=sys.stderr, flush=True)
            traceback.print_exc()
except KeyboardInterrupt:
    pass
finally:
    c.close(); cur.close(); conn.close()
