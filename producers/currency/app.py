import os, time, json, requests
from datetime import datetime, timezone
from confluent_kafka import Producer

BOOT   = os.getenv("KAFKA_BOOTSTRAP","kafka:9092")
TOPIC  = os.getenv("TOPIC","currency.raw")
URL    = os.getenv("FREECURRENCY_URL","https://api.freecurrencyapi.com/v1/latest")
APIKEY = os.getenv("FREECURRENCY_API_KEY")
BASE   = os.getenv("BASE_CURRENCY","USD")
CURRS  = os.getenv("CURRENCIES","EUR,USD,CAD")
SLEEP  = int(os.getenv("FETCH_INTERVAL_SEC","60"))

if not APIKEY:
    raise RuntimeError("FREECURRENCY_API_KEY is missing")

p = Producer({"bootstrap.servers": BOOT})

while True:
    try:
        r = requests.get(URL, params={
            "apikey": APIKEY,
            "base_currency": BASE,
            "currencies": CURRS
        }, timeout=15)
        r.raise_for_status()
        data = r.json().get("data", {})
        fetched_at = datetime.now(timezone.utc).isoformat()
        for symbol, rate in data.items():
            payload = {
                "source": "freecurrencyapi",
                "base_currency": BASE,
                "symbol": symbol,
                "rate": float(rate),
                "fetched_at": fetched_at
            }
            p.produce(TOPIC, key=symbol.encode(), value=json.dumps(payload).encode())
        p.flush()
        print(f"sent {len(data)} rates at {fetched_at}", flush=True)
    except Exception as e:
        print("producer error:", e, flush=True)
    time.sleep(SLEEP)
