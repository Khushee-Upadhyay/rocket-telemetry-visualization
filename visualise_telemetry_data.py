from kafka import KafkaConsumer
import json
import threading
import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import numpy as np
import plotly.graph_objs as go

print("Starting consumer process for real-time visualization")

TOPIC = 'rocket_data'
BROKER = 'localhost:9092'

# Kafka Consumer setup
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BROKER,
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id='dashboard-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Shared data structure
telemetry_data = {"time": [], "altitude": [], "velocity": [], "acceleration": []
                    , "altitude_z_score_anomaly_flag": [], "altitude_z_score": []
                    , "velocity_z_score_anomaly_flag": [], "velocity_z_score": []}
acceleration_data = {"time": [], "acceleration": []}
t_minus_one_velocity = 0
t_minus_one_time = 0
acceleration_data_window_size = 1000

total_flight_duration = 0
min_velocity = 0
max_velocity = 0
max_altitude = 0
first_calc_flag = False

#Z-score anomaly detection data structure
data_window = {"time": [], "altitude": [], "velocity": []}
anomaly_velocity_data = {"time": [], "velocity": []}
anomaly_altitude_data = {"time": [], "altitude": []}
normal_velocity_data = {"time": [], "velocity": []}
normal_altitude_data = {"time": [], "altitude": []}
window_size = 100
threshold = 3

def z_score_calc(data):
    # print("entered z_score_calc function")
    avg_velocity = np.nanmean(data["velocity"])
    stddev_velocity = np.nanstd(data["velocity"])
    z_score_velocity = [(i - avg_velocity)/stddev_velocity if (i is not None) else None for i in data["velocity"] ]

    avg_altitude = np.nanmean(data["altitude"])
    stddev_altitude = np.nanstd(data["altitude"])
    z_score_altitude = [(i - avg_altitude)/stddev_altitude if (i is not None) else None for i in data["altitude"] ]

    return z_score_velocity[-1], z_score_altitude[-1]

# Kafka data fetcher function
def fetch_kafka_data():

    for message in consumer:
        data = message.value
        if data.get("status") == "END":
            print("End of stream encountered.")
            print(data.get("status_time"))
            consumer.close()
            break

        #Calculation of Acceleration
        global t_minus_one_velocity, t_minus_one_time, telemetry_data, acceleration_data
        global first_calc_flag, total_flight_duration, min_velocity, max_velocity, max_altitude
        t_velocity = data["velocity"]
        t_time = data["time"]
        t_acceleration = (t_velocity - t_minus_one_velocity) / (t_time-t_minus_one_time) if (t_velocity is not None) and (t_minus_one_velocity is not None) and (t_time-t_minus_one_time)!=0 else None
        t_altitude = data["altitude"]


        telemetry_data["time"].append(t_time)
        telemetry_data["altitude"].append(t_altitude)
        telemetry_data["velocity"].append(t_velocity)
        telemetry_data["acceleration"].append(t_acceleration)

        acceleration_data["time"].append(t_time)
        acceleration_data["acceleration"].append(t_acceleration)

        t_nan_altitude = t_altitude if t_altitude is not None else np.nan
        t_nan_velocity = t_velocity if t_velocity is not None else np.nan
        data_window["time"].append(t_time)
        data_window["altitude"].append(t_nan_altitude)
        data_window["velocity"].append(t_nan_velocity)

        """
        if len(telemetry_data["time"])<=2000:
            acceleration_data["time"] = telemetry_data["time"]
            acceleration_data["acceleration"] = telemetry_data["acceleration"]

        else:
            acceleration_data["time"] = telemetry_data["time"][-2000:]
            acceleration_data["acceleration"] = telemetry_data["acceleration"][-2000:]
        """
        if len(acceleration_data["time"])>acceleration_data_window_size:
            acceleration_data["time"].pop(0)
            acceleration_data["acceleration"].pop(0)

        t_minus_one_time = t_time
        t_minus_one_velocity = t_velocity
        total_flight_duration = t_time

        #Calculation of summary statistics
        if first_calc_flag == False:

            min_velocity = t_velocity
            max_velocity = t_velocity
            max_altitude = t_altitude
            first_calc_flag = True
        else:
            min_velocity = t_velocity if (t_velocity is not None) and (t_velocity<min_velocity) else min_velocity
            max_velocity = t_velocity if (t_velocity is not None) and (t_velocity>=max_velocity) else max_velocity
            max_altitude = t_altitude if (t_altitude is not None) and (t_altitude >= max_altitude) else max_altitude

        #Calculation of z-score
        if len(data_window["time"])> window_size:
            data_window["time"].pop(0)
            data_window["altitude"].pop(0)
            data_window["velocity"].pop(0)

        if len(data_window["time"])== window_size:
            # print("window size is qual to data window size")
            # print(f"Time: {data_window['time'][-1]}, Altitude: {data_window['altitude'][-1]}, Velocity; {data_window['velocity'][-1]}")
            z_score_velocity, z_score_altitude = z_score_calc(data_window)

            if abs(z_score_velocity) > threshold:
                telemetry_data["velocity_z_score_anomaly_flag"].append(1)
                telemetry_data["velocity_z_score"].append(z_score_velocity)
                anomaly_velocity_data["time"].append(t_time)
                anomaly_velocity_data["velocity"].append(t_velocity)
            else:
                telemetry_data["velocity_z_score_anomaly_flag"].append(0)
                telemetry_data["velocity_z_score"].append(z_score_velocity)
                normal_velocity_data["time"].append(t_time)
                normal_velocity_data["velocity"].append(t_velocity)

            if abs(z_score_altitude) > threshold:
                telemetry_data["altitude_z_score_anomaly_flag"].append(1)
                telemetry_data["altitude_z_score"].append(z_score_altitude)
                anomaly_altitude_data["time"].append(t_time)
                anomaly_altitude_data["altitude"].append(t_altitude)
            else:
                telemetry_data["altitude_z_score_anomaly_flag"].append(0)
                telemetry_data["altitude_z_score"].append(z_score_altitude)
                normal_altitude_data["time"].append(t_time)
                normal_altitude_data["altitude"].append(t_altitude)

        else:
            telemetry_data["velocity_z_score_anomaly_flag"].append(None)
            telemetry_data["velocity_z_score"].append(None)
            normal_velocity_data["time"].append(t_time)
            normal_velocity_data["velocity"].append(t_velocity)
            normal_altitude_data["time"].append(t_time)
            normal_altitude_data["altitude"].append(t_altitude)

        #print data
        #print("Time: {0}, acceleration: {1}".format(t_time, t_acceleration))
        print("time: {}, Telemetry {}, Acceleration; {}, data window: {}, Max velocity; {}, normal altitude: {}, anonmaly altitude; {}".format(t_time, \
                                                                                                   len(telemetry_data["time"]), \
                                                                                                   len(acceleration_data["time"]), \
                                                                                                   len(data_window["time"]), \
                                                                                                   max_velocity, normal_altitude_data["altitude"][-1], len(anomaly_altitude_data["altitude"])))

# Run the consumer in a background thread
thread = threading.Thread(target=fetch_kafka_data)
thread.daemon = True
thread.start()

print("Kafka consumer initiated")


# Initialize Dash app
app = dash.Dash(__name__)

# App layout
app.layout = html.Div([
    html.Div([
            html.H1("Rocket Telemetry Dashboard",
                    style={"margin": "8px 0", "textAlign": "center",
                             "font-size": "2rem",
                             "background-color": "#f4f4f4",
                             "padding": "5px",
                             "box-shadow": "0px 2px 5px rgba(0,0,0,0.1)"}
                    )],
            style={
                "height": "8vh"
}
    ),
    html.Div([
            html.Div([
                dcc.Graph(id='altitude-graph'),
            ],  style={
                        "border": "1px solid #ccc",
                        "border-radius": "10px",
                        "box-shadow": "2px 2px 5px rgba(0,0,0,0.2)",
                        "padding": "10px",
                        "margin": "10px",
                        "background-color": "#f9f9f9",
                        "width": "40%",
                        "display": "inline-block",
                        "vertical-align": "top"
            }),
            html.Div([
                dcc.Graph(id='velocity-graph'),
            ],  style={
                        "border": "1px solid #ccc",
                        "border-radius": "10px",
                        "box-shadow": "2px 2px 5px rgba(0,0,0,0.2)",
                        "padding": "10px",
                        "margin": "10px",
                        "background-color": "#f9f9f9",
                        "width": "40%",
                        "display": "inline-block",
                        "vertical-align": "top"
            }),
    ], style={
                "display": "flex",
                "justify-content": "center",
                "align-items": "center"
    }),
    html.Div([
        html.Div([
                dcc.Graph(id='acceleration-graph', style={'height': '45vh'})
            ],  style={
                        "border": "1px solid #ccc",
                        "border-radius": "10px",
                        "box-shadow": "2px 2px 5px rgba(0,0,0,0.2)",
                        "padding": "10px",
                        "margin": "10px",
                        "background-color": "#f9f9f9",
                        "width": "40%",
                        "display": "inline-block"
            }),
        html.Div([
                html.H2("Rocket Telemetry Summary"),
                html.P(id='max-height', children="Loading..."),
                html.P(id='max-velocity', children="Loading..."),
                html.P(id='min-velocity', children="Loading..."),
                html.P(id='total-flight-time', children="Loading...")
             ],  style={
                        "border": "1px solid #ccc",
                        "border-radius": "10px",
                        "box-shadow": "2px 2px 5px rgba(0,0,0,0.2)",
                        "padding": "10px",
                        "margin": "10px",
                        "background-color": "#f9f9f9",
                        "width": "40%",
                        "display": "inline-block",
                        "vertical-align": "top"
            }),
    ], style={
                "display": "flex",
                "justify-content": "center"
    }),

    dcc.Interval(id='update-interval', interval=1000, n_intervals=0)  # Updates every second
    #dcc.Interval(id='interval-component', interval=5000, n_intervals=0)
], style={
    "height": "100vh"
})

#, Output('acceleration-graph', 'figure')

@app.callback(
    [Output('altitude-graph', 'figure'), Output('velocity-graph', 'figure'),Output('acceleration-graph', 'figure')],
    #[Output('acceleration-graph', 'figure')],
    [Input('update-interval', 'n_intervals')]
)
def update_graphs(n):
    # Altitude graph
    altitude_fig = go.Figure()
    altitude_fig.add_trace(go.Scatter(
        x=normal_altitude_data["time"],
        y=normal_altitude_data["altitude"],
        mode='lines',
        name='Altitude',
        line=dict(color='blue')
    ))
    altitude_fig.add_trace(go.Scatter(
         x=anomaly_altitude_data["time"],
         y=anomaly_altitude_data["altitude"],
         mode='markers',
         name='Anomaly_Altitude',
         marker=dict(size=6, color='red', symbol='circle')
     ))

    altitude_fig.update_layout(title="Altitude vs Time", xaxis_title="Time", yaxis_title="Altitude (km)")

    # Velocity graph
    velocity_fig = go.Figure()
    velocity_fig.add_trace(go.Scatter(
        x=normal_velocity_data["time"],
        y=normal_velocity_data["velocity"],
        mode='lines',
        name='Anomaly_Velocity',
        line=dict(color='green')
    ))
    velocity_fig.add_trace(go.Scatter(
         x=anomaly_velocity_data["time"],
         y=anomaly_velocity_data["velocity"],
         mode='markers',
         name='Velocity',
         marker=dict(size=6, color='red', symbol='circle')
     ))
    velocity_fig.update_layout(title="Velocity vs Time", xaxis_title="Time", yaxis_title="Velocity (m/s)")

    #Acceleration graph
    #print(acceleration_data)
    acceleration_fig = go.Figure()
    acceleration_fig.add_trace(go.Scatter(
        x=acceleration_data["time"],
        y=acceleration_data["acceleration"],
        mode='lines',
        name='Acceleration',
        line=dict(color='red')
    ))
    acceleration_fig.update_layout(title="Acceleration vs Time", xaxis_title="Time", yaxis_title="Acceleration (m/s)/s")

    return [altitude_fig, velocity_fig, acceleration_fig]
    #return [acceleration_fig]

@app.callback(
    [Output('max-height', 'children'), Output('max-velocity', 'children'),Output('min-velocity', 'children'),Output('total-flight-time', 'children')],
    [Input("update-interval", "n_intervals")]
)
def update_summary_widget(n):
    return (
        f"Highest Altitude reached: {max_altitude} kms.",
        f"Maximum Velocity achieved: {max_velocity} m/s.",
        f"Minimum Velocity achieved: {min_velocity} m/s.",
        f"Total flight duration; {total_flight_duration/60} mins. "
    )

if __name__ == '__main__':
    app.run_server(debug=True, host='127.0.0.1', port=8050)