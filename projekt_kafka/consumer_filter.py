from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

for message in consumer:
    tx = message.value
    
    if tx['amount'] > 3000:
        tx['risk_level'] = "HIGH"
    elif tx['amount'] > 1000:
        tx['risk_level'] = "MEDIUM"
    else:
        tx['risk_level'] = "LOW"
    
    if tx['amount'] > 1000:
        print("ALERT", tx)
    else:
        print(tx)
