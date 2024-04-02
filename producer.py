import random
import sys
import time
from datetime import datetime,timedelta
from json import dumps
import uuid  
from cassandra.cluster import Cluster
from kafka import KafkaProducer

CASSANDRA_HOST = 'localhost'
CASSANDRA_KEYSPACE = 'user_interactions'
CASSANDRA_TABLE = 'events'
KAFKA_BOOTSTRAP_SERVER = 'localhost:29092'

def create_keyspace_and_table():
    cluster = Cluster([CASSANDRA_HOST])
    session = cluster.connect()

    # Create keyspace if not exists
    session.execute(f"CREATE KEYSPACE IF NOT EXISTS {CASSANDRA_KEYSPACE} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': '1'}}")

    # Switch to the keyspace
    session.set_keyspace(CASSANDRA_KEYSPACE)

    # Create table if not exists
    session.execute(f"CREATE TABLE IF NOT EXISTS {CASSANDRA_TABLE} (event_id uuid PRIMARY KEY, event_type text, timestamp timestamp, user_id int, page_id int)")

def get_last_event_id():
    cluster = Cluster([CASSANDRA_HOST])
    session = cluster.connect(CASSANDRA_KEYSPACE)
    query = f"SELECT MAX(event_id) AS last_event_id FROM {CASSANDRA_TABLE}"
    result = session.execute(query)
    last_event_id = result.one().last_event_id
    return last_event_id

def produce_message():
    event_types = ['page_visit', 'click', 'purchase', 'signup']
    user_id = random.randint(1, 10000)
    page_id = random.randint(1, 100)
    start_date = datetime.now() - timedelta(days=30)  # Start date for timestamp
    end_date = datetime.now()  # End date for timestamp (current time)
    random_date = start_date + (end_date - start_date) * random.random()  # Random date within the range
    timestamp = random_date.strftime("%Y-%m-%d %H:%M:%S")
    event_type = random.choice(event_types)
    
    message = {
        "event_type": event_type,
        "timestamp": timestamp,
        "user_id": user_id,
        "page_id": page_id
    }
    return message

def main():
    create_keyspace_and_table()

    KAFKA_TOPIC = sys.argv[1]
    producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
                            value_serializer=lambda x: dumps(x).encode('utf-8'))
    last_event_id = get_last_event_id()
    print("Kafka producer application started.")
    
    try:
        while True:
            message = produce_message()
            print(f"Produced message: {message}")
            producer.send(KAFKA_TOPIC, message)
            interval = random.uniform(1, 5)
            time.sleep(interval)
    except KeyboardInterrupt:
        producer.flush()
        producer.close()
        print("Kafka producer application completed.")

if __name__ == "__main__":
    main()
