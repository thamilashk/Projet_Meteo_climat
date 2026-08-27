import time
import json
import requests
from kafka import KafkaProducer

# Connexion au serveur Kafka
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TOPIC_NAME = 'meteo-realtime'
API_URL = "https://api.open-meteo.com/v1/forecast?latitude=48.8566&longitude=2.3522&current_weather=true"

print(f"Lancement du producteur Kafka sur le topic '{TOPIC_NAME}'...")

try:
    while True:
        try:
            response = requests.get(API_URL, timeout=10)
            if response.status_code == 200:
                payload = response.json().get("current_weather", {})
                payload["station_id"] = "PARIS_01"
                payload["latitude"] = 48.8566
                payload["longitude"] = 2.3522
                
                # Envoi et validation immédiate dans Kafka
                producer.send(TOPIC_NAME, value=payload)
                producer.flush() 
                print(f"[Kafka OK] Station: PARIS_01 | Temp: {payload.get('temperature')}°C | Heure: {payload.get('time')}")
            else:
                print(f"Erreur API: {response.status_code}")
        except Exception as e:
            print(f"Erreur de connexion : {e}")
            
        time.sleep(10)
except KeyboardInterrupt:
    print("\nProducteur arrêté.")