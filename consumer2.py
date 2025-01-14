from kafka import KafkaConsumer
from json import loads
import os
import sys
from pyspark.sql import SparkSession


os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
spark = SparkSession.builder.appName("Spark consumer").getOrCreate()

print("Starting consumer process for persistent data storage using pyspark.")

TOPIC = 'rocket_data'
BROKER = 'localhost:9092'
CURRENT_DIR = os.getcwd()
OUTPUT_DIR = './parquet_data2/'

os.makedirs(OUTPUT_DIR, exist_ok=True)

OUTPUT_DIR = 'file://'+CURRENT_DIR+'/parquet_data2/'

consumer = KafkaConsumer(
     TOPIC,
     bootstrap_servers=BROKER,
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='persistent-storage-group',
     value_deserializer=lambda x: loads(x.decode('utf-8')))

print("Kafka consumer initiated")

data_buffer = []
columns = ["time", "velocity", "altitude"]
BUFFER_LIMIT = 1000

FILE_INDEX = 1

print("Data buffer initiated")
for message in consumer:
    msg = message.value
    if msg.get('status') == 'END':
        print("Received END message")
        df = spark.createDataFrame(data_buffer, columns)
        output_file = os.path.join(OUTPUT_DIR, f'rocket_data_{FILE_INDEX}.parquet')
        df.write.parquet(output_file)

        print(f'Written data buffer of length {len(data_buffer)} to the output file: {output_file}')
        print("Stopping Consumer")
        break

    data_buffer.append((msg["time"], msg["velocity"], msg["altitude"]))

    if (len(data_buffer) >= BUFFER_LIMIT-1) :
        df = spark.createDataFrame(data_buffer, columns)
        output_file = os.path.join(OUTPUT_DIR, f'rocket_data_{FILE_INDEX}.parquet')
        df.write.parquet(output_file)

        print(f'Written data buffer of length {len(data_buffer)} to the output file: {output_file}')

        FILE_INDEX += 1
        data_buffer = []

consumer.close()