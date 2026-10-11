import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

# 1. Attach directly to your running Docker Kafka engine endpoint
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TOPIC_NAME = 'realtime_security_stream'
IP_POOL = ['192.168.1.102', '10.0.0.55', '172.16.9.12', '185.220.101.5']

print("🚀 Real-Time Kafka Stream Producer Online. Broadcasting telemetry feeds...")

try:
    while True:
        is_attack = random.random() < 0.15
        
        # THIS IS THE VITAL BLOCK THAT DEFINES LOG_PAYLOAD SO LINE 19 WORKS!
        log_payload = {
            "timestamp": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
            "source_ip": '185.220.101.5' if is_attack else random.choice(IP_POOL),
            "user_identity": 'admin_root' if is_attack else 'regular_user',
            "failed_logins": random.randint(12, 35) if is_attack else random.randint(0, 2),
            "data_transferred_mb": round(random.uniform(550.0, 1650.0), 2) if is_attack else round(random.uniform(0.5, 30.0), 2)
        }
        
        # Broadcast the payload to the Kafka cluster
        producer.send(TOPIC_NAME, log_payload)
        
        # This matches your print formatting perfectly, but has the full data structure available!
        print(f"Source: {log_payload['source_ip']} | Logins: {log_payload['failed_logins']} | Data: {log_payload['data_transferred_mb']} MB")
        
        time.sleep(1) # Feed live updates continuously every second

except KeyboardInterrupt:
    print("\n🛑 Telemetry Generation Halted Safely.")