import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer


producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

stations = [
    {"id": "JP-TOKYO", "lat": 35.6895, "lon": 139.6917},
    {"id": "JP-OSAKA", "lat": 34.6937, "lon": 135.5023},
    {"id": "JP-NAGOYA", "lat": 35.1815, "lon": 136.9066}
]

def generate_magnitude():
    alpha, beta = 4, 8
    raw = random.betavariate(alpha, beta)  # 0–1
    magnitude = raw * 10.0
    return round(magnitude, 2)


def generate_seismic_data(station):
    magnitude = generate_magnitude()
    depth = round(random.triangular(20, 70, 30), 1)
    return {
        "station_id": station["id"],
        "timestamp": datetime.utcnow().isoformat(),
        "location": {"lat": station["lat"], "lon": station["lon"]},
        "magnitude": magnitude,
        "depth_km": depth,
        "frequency_hz": round(random.uniform(0.5, 10.0), 2)
    }

def run_simulation():
    try:
        while True:
            station = random.choice(stations)
            reading = generate_seismic_data(station)
            print(f"Sending: {reading}")
            producer.send('seismic_readings', reading)
            producer.flush()  # wymuszamy wysłanie
            time.sleep(1)
    except KeyboardInterrupt:
        print("Symulacja zatrzymana przez użytkownika.")
    finally:
        producer.close()

if __name__ == "__main__":
    run_simulation()
