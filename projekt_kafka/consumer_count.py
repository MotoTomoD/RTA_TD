from kafka import KafkaConsumer
from collections import Counter, defaultdict
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',
    group_id='count-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

store_counts = Counter()
total_amount = defaultdict(float)
msg_count = 0

for message in consumer:
    tx = message.value
    store = tx['store']
    amount = tx['amount']

    msg_count += 1
    store_counts[store] += 1
    total_amount[store] += amount

    if msg_count % 10 == 0:
        print(f"\n--- PODSUMOWANIE (WIADOMOSCI: {msg_count}) ---")
        print(f"{'SKLEP':<15} | {'LICZBA':<10} | {'SUMA KWOT':<15}")
        print("-" * 45)
        for s in sorted(store_counts.keys()):
            print(f"{s:<15} | {store_counts[s]:<10} | {total_amount[s]:>10.2f} PLN")
        print("-" * 45)
