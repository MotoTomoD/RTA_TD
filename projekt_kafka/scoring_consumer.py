from kafka import KafkaConsumer, KafkaProducer
import json

def score_transaction(tx):
    score = 0
    if tx['amount'] > 3000:
        score += 3
    
    if tx.get('category') == 'elektronika' and tx['amount'] > 1500:
        score += 2
    
    if tx.get('hour', 24) < 6:
        score += 2
        
    return score

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',
    group_id='scoring-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

alert_producer = KafkaProducer(
    bootstrap_servers='broker:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

for message in consumer:
    tx = message.value
    score = score_transaction(tx)
    
    if score >= 3:
        tx['score'] = score
        alert_producer.send('alerts', value=tx)
        print("ALERT", tx)
