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
---

## How does it work?
1. **Setup**
   - Set up a kafka topic with the following command:
     ```kafka-topics.sh --create --topic rocket_data --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1```
   - You can start the producer run using step 2 below and then steps 3 and 4 can be executed in parallel as it would be the case in a real-life setting.

2. **Data Simulation**
   - The producer script (_**producer.py**_) generates telemetry data points (time, altitude, velocity) from a json file stored in _**data**_ folder and writes it to a kafka topic (rocket_data) with a delay of 0.01 seconds.
   - This script runs in 2 modes - normal and anomaly.
       - _Normal mode_: All the data points generated from the original data file passed into the kafka topic as it is.
       - _Anomaly mode_: Injects three types of anomalies - Gaussian noise, spike anomaly and sensor failure at random data points and passes into the kafka topic.
       - When run in anomaly mode, the generated data is stored as a csv in the _**producer_data**_ folder for later analysis.
   - Run this file in the terminal with the following commands:
       - _Normal mode_: ```python3 producer.py normal "data/stage2 raw.json"```
       - _Anomaly mode_: ```python3 producer.py anomaly_generator "data/stage2 raw.json"```
   - The script can be stopped in the middle using Ctrl+C and it has mechanisms for writing the data read till that point to the kafka topic before exiting.

   
3. **Data Persistence**
   - The consumer scripts (_**consumer.py**_ and _**consumer2.py**_) read data from the kafka topic and write to _**parquet_data**_ and -**parquet_data2**_ folders respectively.
   - _**consumer.py**_ uses Pandas and PyArrow to manipulate the read data into a parquet format write to the _**parquet_data**_ folder if no argument is provided or to the folder provided in the argument.
   - _**consumer2.py**_ uses Pyspark to achieve the same results.
   - Both the scripts can be stopped in the middle using Ctrl+C and they have mechanisms for writing the data read till that point to the designated folders before exiting.
   - Run these files in the terminal with the following commands:
       - ```python3 consumer.py```
       - ```python3 consumer.py “./parquet_data3”```
       - ```python3 consumer2.py```
    
4. **Real-time data Visualization**
   - Vizualize telemetry data in real-time using the command below:
       - ```python3 visualise_telemetry_data.py```
    
   - Navigate to ```http://127.0.0.1:8050/ ``` to visualize the results.
 

5. **Results**
   - Altitude v/s time graph:
      ![Screenshot_2025-01-18_15-47-17 (copy 1)](https://github.com/user-attachments/assets/9445ecc3-9c95-4fd1-bab0-383a54be2b4d)

   - Velocity v/s time graph:
      ![Screenshot_2025-01-18_15-47-17 (copy 2)](https://github.com/user-attachments/assets/918c834e-1f62-4b0b-89b3-622a30a75492)

   - Acceleration v/s time graph:
      ![Screenshot_2025-01-18_15-48-29 (copy 1)](https://github.com/user-attachments/assets/2757e768-3da9-4ffe-bffc-2c259101da8e)

   - Dashboard: 
      ![Screenshot_2025-01-18_15-48-29 (copy 2)](https://github.com/user-attachments/assets/11e88af7-6911-46b6-b8ec-b1adce76f727)

     

     
---

## Future Enhancements
- Add more sophisticated anomaly detection techniques like machine-learning based or clustering based techniques.
- Implement predictive analytics for trajectory estimation.









