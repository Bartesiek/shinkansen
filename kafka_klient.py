from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'seismic_readings',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id='seismic-analyzer',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Czekam na dane...")

for message in consumer:
    data = message.value
    magnitude = data['magnitude']
    station_id = data['station_id']
    timestamp = data['timestamp']

    if magnitude >= 5.0:
        print(f"[ALERT] Silne trzęsienie ziemi! Stacja: {station_id}, Magnituda: {magnitude}, Czas: {timestamp}. Wysłano polecenie zatrzymania się do pociągów w pobliżu")
    else:
        print(f"Odczyt z {station_id} | Magnituda: {magnitude}")
