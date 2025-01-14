import numpy as np
import pandas as pd

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

def rolling_z_score_calc(data_df):
    #filter_df = data_df[data_df['anomaly_flg']=='N'].tail(100) #Including anomalous records
    filter_df = data_df.tail(window_size)
    mean = filter_df["velocity"].mean()
    std_dev = filter_df["velocity"].std()
    z_score_val = (data_df["velocity"].iloc[-1] - mean)/std_dev if (data_df["velocity"].iloc[-1] is not None) else None

    if (filter_df.shape[0]>window_size) and ((z_score_val is None) or abs(z_score_val)>=threshold):
        data_df["anomaly_flg"].iloc[-1] = "Y"
        data_df["anomaly_detection_technique"].iloc[-1] = "Z-Score"
        data_df["velocity_anomaly_flag"].iloc[-1] = "Y"
        data_df["velocity_anomaly_score"].iloc[-1] = z_score_val
    else:
        data_df["anomaly_flg"].iloc[-1] = "N"
        data_df["anomaly_detection_technique"].iloc[-1] = "NA"
        data_df["velocity_anomaly_flag"].iloc[-1] = "N"
        data_df["velocity_anomaly_score"].iloc[-1] = z_score_val

    mean = filter_df["altitude"].mean()
    std_dev = filter_df["altitude"].std()
    z_score_val = (data_df["altitude"].iloc[-1] - mean) / std_dev if (data_df["altitude"].iloc[-1] is not None) else None

    if (filter_df.shape[0]>window_size) and ((z_score_val is None) or abs(z_score_val)>=threshold):
        data_df["anomaly_flg"].iloc[-1] = "Y"
        data_df["anomaly_detection_technique"].iloc[-1] = "Z-Score"
        data_df["altitude_anomaly_flag"].iloc[-1] = "Y"
        data_df["altitude_anomaly_score"].iloc[-1] = z_score_val
    else:
        data_df["anomaly_flg"].iloc[-1] = "N"
        data_df["anomaly_detection_technique"].iloc[-1] = "NA"
        data_df["altitude_anomaly_flag"].iloc[-1] = "N"
        data_df["altitude_anomaly_score"].iloc[-1] = z_score_val

    return data_df