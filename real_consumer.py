import json
from kafka import KafkaConsumer
from pymongo import MongoClient

mongo_client = MongoClient('mongodb://localhost:27017/')
db = mongo_client['siem_data_warehouse']
collection = db['processed_security_logs']

consumer = KafkaConsumer(
    'realtime_security_stream',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='latest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("🔒 Automated Security SIEM Consumer Online. Watching for streaming telemetry alerts...")

for message in consumer:
    log_event = message.value
    
    is_anomaly = False
    incidents = []
    
    # Secure fallback checks mapping prevents any unexpected KeyError crashes
    failed_logins = log_event.get('failed_logins', 0)
    data_transferred_mb = log_event.get('data_transferred_mb', 0.0)
    
    if failed_logins >= 5:
        is_anomaly = True
        incidents.append("Brute-Force Anomaly Detected")
    if data_transferred_mb > 400.0:
        is_anomaly = True
        incidents.append("High-Volume Data Exfiltration Vector")
        
    log_event['threat_flagged'] = is_anomaly
    log_event['incident_tags'] = incidents if is_anomaly else ["Baseline Normal"]
    
    collection.insert_one(log_event)
    
    if is_anomaly:
        print(f"\n⚠️  [REAL-TIME ALERT IDENTIFIED] ⚠️")
        print(f"🚨 Anomalous Host IP: {log_event.get('source_ip')} | Target Identity: {log_event.get('user_identity')}")
        print(f"❌ Violations: {', '.join(incidents)}")
        print(f"📂 Record successfully logged to MongoDB cluster.\n")
    else:
        print(f"✅ Telemetry Packet {log_event.get('source_ip')} parsed cleanly. Status: SECURE.")