from kafka import KafkaConsumer
from json import loads
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os
import sys

print("Starting consumer process for persistent data storage")

TOPIC = 'rocket_data'
BROKER = 'localhost:9092'
OUTPUT_DIR = './parquet_data/'

if len(sys.argv) == 2:
    OUTPUT_DIR = sys.argv[1]

os.makedirs(OUTPUT_DIR, exist_ok=True)

consumer = KafkaConsumer(
     TOPIC,
     bootstrap_servers=BROKER,
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='persistent-storage-group',
     value_deserializer=lambda x: loads(x.decode('utf-8')))

print("Kafka consumer initiated")

data_buffer = []
BUFFER_LIMIT = 1000

FILE_INDEX = 1

print("Data buffer initiated")
for message in consumer:
    msg = message.value
    if msg.get('status') == 'END':
        print(f"Received END message with status time: {msg.get('status_time')}")
        df = pd.DataFrame(data_buffer)
        tbl = pa.Table.from_pandas(df)
        output_file = os.path.join(OUTPUT_DIR, f'rocket_data_{FILE_INDEX}.parquet')
        pq.write_table(tbl, output_file)

        print(f'Written data buffer of length {len(data_buffer)} to the output file: {output_file}')
        print("Stopping Consumer")
        break

    data_buffer.append(msg)
    if (len(data_buffer) >= BUFFER_LIMIT-1) :

        df = pd.DataFrame(data_buffer)
        tbl = pa.Table.from_pandas(df)
        output_file = os.path.join(OUTPUT_DIR, f'rocket_data_{FILE_INDEX}.parquet')
        pq.write_table(tbl, output_file)

        print(f'Written data buffer of length {len(data_buffer)} to the output file: {output_file}')

        FILE_INDEX += 1
        data_buffer = []

consumer.close()
