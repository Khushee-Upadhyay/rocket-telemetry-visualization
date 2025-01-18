# Rocket Telemetry Data Generation, Anomaly Detection and Visualization 🚀

This project simulates real-time rocket telemetry data generation, anomaly detection and visualizing key metrics for tracking rocket behavior.

---

## Features
- **Real-time Telemetry Data Generation:**
  - Existing Telemetry data for [Iridium NEXT 1 launch](https://github.com/shahar603/Telemetry-Data/tree/master/Iridium%20NEXT%201) is used to simulate real-time data generation.
  - Kafka topics used to produce data in normal and anomaly mode.
- **Anomaly Detection:**
  - Implements z-score anomaly detection technique to identify unusual data patterns.
  - Highlights anomalies directly on the dashboard.
- **Real-time Visualization:**
  - Displays key metrics like altitude, velocity and acceleration in real-time.
  - Interactive charts for detailed analysis.
- **Data persistencec:**
  - Persisting data in parquet format using Spark and Pandas/PyArrow for offline analysis and review.
---

## Technologies used
- **Programming Language:** Python
- **Message Broker:** Apache Kafka
- **Data Manipulation Libraries:** Pandas, NumPy, PyArrow
- **Data Storage:** Parquet
- **Big Data Framework:** Spark
- **Dashboard Framework:** Dash by Plotly
- **Data Visualization:** Plotly
