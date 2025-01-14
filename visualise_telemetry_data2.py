from kafka import KafkaConsumer
import json, os
import signal
import threading
import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.graph_objs as go
from anomaly_detection_methods import rolling_z_score_calc
import datetime
import pandas as pd

print("Starting consumer process for real-time visualization")

TOPIC = 'rocket_data'
BROKER = 'localhost:9092'
OUTPUT_DIR = './visualized_data/'

os.makedirs(OUTPUT_DIR, exist_ok=True)

# Kafka Consumer setup
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BROKER,
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id='dashboard-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

def signal_handler(sig, frame):
    print('\nYou pressed Ctrl+C!')
    # Perform any cleanup or other actions here
    ct = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    data = {"status": "END", "status_time": ct}

    file_name = os.path.join(OUTPUT_DIR, f'viz_telemetry_data_{ct}.csv')
    telemetry_data_df.to_csv(file_name, sep=',', index=False)
    file_name = os.path.join(OUTPUT_DIR, f'viz_acceleration_data_{ct}.csv')
    acceleration_data_df.to_csv(file_name, sep=',', index=False)
    file_name = os.path.join(OUTPUT_DIR, f'viz_summary_data_{ct}.csv')
    summary_data_df.to_csv(file_name, sep=',', index=False)

    print("End of stream encountered.")
    print(data.get("status_time"))
    consumer.close()
    exit(0)

# Set the signal handler for SIGINT (Ctrl+C)
signal.signal(signal.SIGINT, signal_handler)

# Shared data structure
telemetry_data_df = pd.DataFrame(columns={"time", "altitude", "velocity",
                                          "anomaly_flg", "anomaly_detection_technique",
                                          "altitude_anomaly_flag", "altitude_anomaly_score",
                                          "velocity_anomaly_flag", "velocity_anomaly_score"})
acceleration_data_df = pd.DataFrame(columns={"time", "acceleration"})
summary_data_df = pd.DataFrame(columns={"max_velocity", "min_velocity", "max_altitude", "total_flight_duration"})

window_size = 100
threshold = 3

def fetch_kafka_data():
    print("inside fetch kafka data function")
    global telemetry_data_df, acceleration_data_df, summary_data_df
    for message in consumer:
        data = message.value
        print(data)
        if data.get("status") == "END":
            print("End of stream encountered.")
            print(data.get("status_time"))
            ct = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            file_name = os.path.join(OUTPUT_DIR, f'viz_telemetry_data_{ct}.csv')
            telemetry_data_df.to_csv(file_name, sep=',', index=False)
            file_name = os.path.join(OUTPUT_DIR, f'viz_acceleration_data_{ct}.csv')
            acceleration_data_df.to_csv(file_name, sep=',', index=False)
            file_name = os.path.join(OUTPUT_DIR, f'viz_summary_data_{ct}.csv')
            summary_data_df.to_csv(file_name, sep=',', index=False)

            consumer.close()
            break

        t_velocity = data["velocity"]
        t_time = data["time"]
        t_altitude = data["altitude"]


        #Anomaly Detection

        if telemetry_data_df.shape[0]<window_size:
            datadict = dict(time=t_time, altitude=t_altitude, velocity=t_velocity, anomaly_flg="N",
                        anomaly_detection_technique="NA", altitude_anomaly_flag="N", altitude_anomaly_score=None,
                        velocity_anomaly_flag="N", velocity_anomaly_score=None)
            telemetry_data_df = telemetry_data_df.append(datadict, ignore_index=True)

        else:
            datadict = dict(time=t_time, altitude=t_altitude, velocity=t_velocity, anomaly_flg="N",
                        anomaly_detection_technique=None, altitude_anomaly_flag=None, altitude_anomaly_score=None,
                        velocity_anomaly_flag=None, velocity_anomaly_score=None)
            telemetry_data_df = telemetry_data_df.append(datadict, ignore_index=True)
            telemetry_data_df = rolling_z_score_calc(telemetry_data_df)

        #Acceleration calculation
        df1 = telemetry_data_df[telemetry_data_df["anomaly_flg"]=="N"]
        if acceleration_data_df.shape[0]==0:
            datadict = dict(time=df1["time"].iloc[-1], acceleration=0)
            acceleration_data_df = acceleration_data_df.append(datadict, ignore_index=True)
        else:
            t1 = df1["time"].iloc[-1]
            t2 = df1["time"].iloc[-2]
            v1 = df1["velocity"].iloc[-1]
            v2 = df1["velocity"].iloc[-2]
            acc = (v1 - v2) / (t1-t2) if (v1 is not None) and (v2 is not None) and (t1-t2)!=0 else None
            datadict = dict(time=t1, acceleration=acc)
            acceleration_data_df = acceleration_data_df.append(datadict, ignore_index=True)

        #Summmary statistics calculation
        datadict = dict(max_velocity = telemetry_data_df[telemetry_data_df["anomaly_flg"]=="N"]["velocity"].max(skipna=True),
                        min_velocity = telemetry_data_df[telemetry_data_df["anomaly_flg"] == "N"]["velocity"].min(skipna=True),
                        max_altitude = telemetry_data_df[telemetry_data_df["anomaly_flg"] == "N"]["altitude"].max(skipna=True),
                        total_flight_duration = telemetry_data_df[telemetry_data_df["anomaly_flg"] == "N"]["time"].max(skipna=True)
                        )
        summary_data_df = summary_data_df.append(datadict, ignore_index=True)
        print(acceleration_data_df.tail(1))
        print(summary_data_df.tail(1))


# Run the consumer in a background thread
# thread = threading.Thread(target=fetch_kafka_data)
# thread.daemon = True
# thread.start()

print("Kafka consumer initiated")
fetch_kafka_data()