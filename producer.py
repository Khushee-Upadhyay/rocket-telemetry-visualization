import sys
from time import sleep
import datetime
from json import dumps , load
from kafka import KafkaProducer
import signal
import random
import numpy as np
import pandas as pd
import os

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: dumps(x).encode('utf-8'))

"""
https://github.com/shahar603/Telemetry-Data/tree/master

The analysed files contain telemetry that can be calculated from the raw data in 1 second interval.

These files contain the following fields:

Time (s), Velocity (m/s), Altitude (km), Vertical Velocity (m/s), Horizontal Velocity (m/s), Acceleration (m/s^2) (with gravity), Downrange Distance (km), Velocity Angle (degrees), Aerodynamic Pressure (N).

The files contain data from T+0 until the telemetry is cut. Each launch has analysis of either stage 1 or 2. The analysed stage can be found in the Launches file in the field analysed_stage.
"""

def signal_handler(sig, frame):
    print('\nYou pressed Ctrl+C!')
    # Perform any cleanup or other actions here
    ct = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    data = {"status": "END", "status_time": ct}
    producer.send('rocket_data', value=data)
    print(data)
    print("End message sent to kafka topic. Program interrupter manually.")
    if run_mode == "anomaly_generator":
        file_name = os.path.join(OUTPUT_DIR, f"producer_anomaly_run_{ct}.csv")
        df.to_csv(file_name, sep=',', index=False)
        print(f"Dataframe written to file: {file_name}")
    producer.close()
    print("Producer closed.")
    exit(0)

# Set the signal handler for SIGINT (Ctrl+C)
signal.signal(signal.SIGINT, signal_handler)

run_mode = sys.argv[1]
file_path = sys.argv[2]

random.seed(45)
noise_factor = 0.001
spike_factor = 0.5
anomaly_factor_1 = 0.004
anomaly_factor_2 = 0.0005

OUTPUT_DIR = './producer_data/'
os.makedirs(OUTPUT_DIR, exist_ok=True)

try:
    if run_mode == "normal":
        with open(file_path, 'r') as file:
            rdf = load(file)

        for d in zip(rdf['time'], rdf['velocity'], rdf['altitude']):
            # print('Time: {}, Velocity: {}, Altitude: {}'.format(d[0], d[1], d[2]))
            data = {'time': d[0], 'velocity': d[1], 'altitude': d[2]}
            print(data)
            producer.send('rocket_data', value=data)
            sleep(0.01)

        ct = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        data = {"status": "END", "status_time": ct}
        producer.send('rocket_data', value=data)
        print(data)
        print("End message sent to kafka topic. File data end.")
        producer.close()

    elif run_mode == "anomaly_generator":
        with open(file_path, 'r') as file:
            rdf = load(file)

        df = pd.DataFrame(columns=['time', 'velocity', 'altitude', 'anomaly_flg', 'anomaly_desc'])
        anomaly_flg = 'N'
        anomaly_desc = ''

        for d in zip(rdf['time'], rdf['velocity'], rdf['altitude']):
            t, v, a = d[0], d[1], d[2]
            temp = random.random()
            if (temp >= anomaly_factor_1) and (temp< 2*anomaly_factor_1):
                v = v + np.random.normal(0, noise_factor*v)
                a = a + np.random.normal(0, noise_factor*a)
                anomaly_flg = 'Y'
                anomaly_desc += "Gaussian anomaly;"
                print(f"Gaussian anomaly at time {t}")
            elif temp < anomaly_factor_2:
                v = v + spike_factor * random.randrange(100, 500)
                a = a + spike_factor * random.randrange(100, 500)
                anomaly_flg = 'Y'
                anomaly_desc += "Spike anomaly;"
                print(f"Spike anomaly at time {t}")
            elif (temp >= anomaly_factor_2) and (temp < 2*anomaly_factor_2):
                v = None
                anomaly_flg = 'Y'
                anomaly_desc += "Velocity sensor failure;"
                print(f"Velocity sensor failure at time {t}")
            elif (temp >= 2*anomaly_factor_2) and (temp < 3*anomaly_factor_2):
                a = None
                anomaly_flg = 'Y'
                anomaly_desc += "Altitude sensor failure;"
                print(f"Altitude sensor failure at time {t}")

            data = {'time': t, 'velocity': v, 'altitude': a}
            print(data)
            producer.send('rocket_data', value=data)
            df = df.append({'time': t, 'velocity': v, 'altitude': a, 'anomaly_flg': anomaly_flg, 'anomaly_desc': anomaly_desc}, ignore_index=True)
            #print(df.count())
            anomaly_flg = 'N'
            anomaly_desc = ''
            sleep(0.01)

        ct = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        data = {"status": "END", "status_time": ct}
        producer.send('rocket_data', value=data)
        print(data)
        print("End message sent to kafka topic. File data end.")
        file_name = os.path.join(OUTPUT_DIR, f'producer_anomaly_run_{ct}.csv')
        df.to_csv(file_name, sep=',', index=False)
        print(f"Dataframe written to file: {file_name}")
        producer.close()

    else:
        print("Invalid run mode: {}".format(run_mode))
except KeyboardInterrupt:
    print('\nExiting gracefully...')