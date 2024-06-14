from airflow import DAG #Import DAG class from Airflow to create DAG.
from datetime import timedelta, datetime #Import timedelta and datetime classes for time manipulation.
from airflow.providers.http.sensors.http import HttpSensor #Import HttpSensor to check availability of HTTP endpoint.
import json
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator #Import PythonOperator to run Python functions.
import pandas as pd

# converts temperature from Kelvin to Fahrenheit.
def kelvin_to_fahrenheit(temp_in_kelvin):
    temp_in_fahrenheit = (temp_in_kelvin - 273.15) * (9/5) + 32
    return temp_in_fahrenheit


def transform_load_data(task_instance): #here we can specifiy what task we need to provide, in this case, previous task, which is extraction.
    data = task_instance.xcom_pull(task_ids="extract_weather_data") #Pulls data from previous task using xcom_pull.
    city = data["name"]
    weather_description = data["weather"][0]['description']
    temp_farenheit = kelvin_to_fahrenheit(data["main"]["temp"])
    feels_like_farenheit= kelvin_to_fahrenheit(data["main"]["feels_like"])
    min_temp_farenheit = kelvin_to_fahrenheit(data["main"]["temp_min"])
    max_temp_farenheit = kelvin_to_fahrenheit(data["main"]["temp_max"])
    pressure = data["main"]["pressure"]
    humidity = data["main"]["humidity"]
    wind_speed = data["wind"]["speed"]
    time_of_record = datetime.utcfromtimestamp(data['dt'] + data['timezone'])
    sunrise_time = datetime.utcfromtimestamp(data['sys']['sunrise'] + data['timezone'])
    sunset_time = datetime.utcfromtimestamp(data['sys']['sunset'] + data['timezone'])

    transformed_data = {"City": city,
                        "Description": weather_description,
                        "Temperature (F)": temp_farenheit,
                        "Feels Like (F)": feels_like_farenheit,
                        "Minimun Temp (F)":min_temp_farenheit,
                        "Maximum Temp (F)": max_temp_farenheit,
                        "Pressure": pressure,
                        "Humidty": humidity,
                        "Wind Speed": wind_speed,
                        "Time of Record": time_of_record,
                        "Sunrise (Local Time)":sunrise_time,
                        "Sunset (Local Time)": sunset_time                        
                        }
    transformed_data_list = [transformed_data]
    df_data = pd.DataFrame(transformed_data_list)

    now = datetime.now()
    dt_string = now.strftime("%d%m%Y%H%M%S")
    dt_string = 'current_weather_data_fremont_' + dt_string
    df_data.to_csv(f"{dt_string}", index=False)



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email': ['myemail@domain.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,   #Number of retries on failure.
    'retry_delay': timedelta(minutes=2) #Time between retries.
}

# Defines the DAG
with DAG('dag_weather',
        default_args=default_args,
        schedule_interval = '@daily',
        catchup=False) as dag:

        # Defines an HttpSensor task is_weather_api_ready to check if the weather API is available.
        is_weather_api_ready = HttpSensor(
        task_id ='is_weather_api_ready',
        http_conn_id='my_weathermap_api',
        endpoint='/data/2.5/weather?q=Fremont&APPID="YOUR-API-KEY-HERE"'
        # endpoint='/data/3.0/onecall/timemachine?lat={37.2958479}&lon={-121.980906}&appid={API key}'
        )

        # Defines a SimpleHttpOperator task extract_weather_data to make a GET request to the weather API and log the response.
        extract_weather_data = SimpleHttpOperator(
        task_id = 'extract_weather_data',
        http_conn_id = 'my_weathermap_api',
        endpoint='/data/2.5/weather?q=Fremont&APPID="YOUR-API-KEY-HERE"',
        method = 'GET',
        response_filter= lambda r: json.loads(r.text),
        log_response=True
        )

        # Defines a PythonOperator task transform_load_weather_data to run the transform_load_data function.
        transform_load_weather_data = PythonOperator(   #task for transforming and loading data.
        task_id= 'transform_load_weather_data',
        python_callable=transform_load_data #This function will run, for this particular task.
        )




        is_weather_api_ready >> extract_weather_data >> transform_load_weather_data #this is the workflow, to connect 2 vertex.
