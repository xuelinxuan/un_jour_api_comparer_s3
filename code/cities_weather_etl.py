
from airflow import DAG                    # for DAG authoring
from airflow.operators.python import PythonOperator          # To create Custom Python Functions
from airflow.providers.amazon.aws.hooks.s3 import S3Hook                # To interact with S3
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook            # To interact with Snowflake
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator         # To handle Slack messaging
from datetime import datetime, timedelta                                  # To manipulate time and date data
import requests                                     # To send HTTP requests and handle response
import io                            # for dealing with in-memory operations, especially in Pandas and S3
import pandas as pd                                  # for creating and handling dataframes as well as converting to CSV
from airflow.models import Variable            # For retrieving configuratio variables dynamically

# AWS & Snowflake Config
S3_BUCKET = "weatherapi-multiple-cities"
AWS_CONN_ID = "aws_new_conn"
SNOWFLAKE_CONN_ID = "snowflake_new_conn"
SNOWFLAKE_SCHEMA = "cities_weather_schema"
SNOWFLAKE_DATABASE = "cities_weather_database"
SNOWFLAKE_WAREHOUSE = "cities_weather_warehouse"
SNOWFLAKE_STAGE = "cities_weather_stage_area"

# API Key and Slack Webhook URL retrieved securely
API_KEY = Variable.get("openweather_api_key")
SLACK_WEBHOOK_URL = Variable.get("slack_webhook_url")
IAM_ACCESS_KEY = Variable.get("iam_user_access_key")
IAM_SECRET_KEY = Variable.get("iam_user_secret_key")

# City List
CITIES = ["Portland", "Seattle", "London", "Mumbai", "Beijing"]

# Convert Kelvin to Fahrenheit
def kelvin_to_fahrenheit(temp):
    return (temp - 273.15) * 9/5 + 32

# Extract, Transform, and Load Weather Data to S3
def extract_transform_weather_data(city):
    base_url = "https://api.openweathermap.org/data/2.5/weather"
    response = requests.get(f"{base_url}?q={city}&appid={API_KEY}")
    data = response.json()

    # Transform extracted data
    new_record = {
        "City": city,
        "Description": data["weather"][0]["description"],
        "Temperature (F)": kelvin_to_fahrenheit(data["main"]["temp"]),
        "Feels Like (F)": kelvin_to_fahrenheit(data["main"]["feels_like"]),
        "Min Temperature (F)": kelvin_to_fahrenheit(data["main"]["temp_min"]),
        "Max Temperature (F)": kelvin_to_fahrenheit(data["main"]["temp_max"]),
        "Pressure": data["main"]["pressure"],
        "Humidity": data["main"]["humidity"],
        "Wind Speed": data["wind"]["speed"],
        "Time of Record": datetime.utcfromtimestamp(data['dt'] + data['timezone']),
        "Sunrise": datetime.utcfromtimestamp(data['sys']['sunrise'] + data['timezone']),
        "Sunset": datetime.utcfromtimestamp(data['sys']['sunset'] + data['timezone'])
    }

    # Initialize S3 Hook
    s3_hook = S3Hook(AWS_CONN_ID)
    csv_filename = f"{city.lower()}.csv"

    # Check if the file exists in S3
    if s3_hook.check_for_key(csv_filename, S3_BUCKET):
        # Read existing CSV file from S3
        s3_file = s3_hook.read_key(csv_filename, S3_BUCKET)
        df = pd.read_csv(io.StringIO(s3_file))

        # Ensure "Time of Record" column is a string to avoid mismatches
        df["Time of Record"] = df["Time of Record"].astype(str)

        # Check if new record already exists in S3 CSV file
        if not df["Time of Record"].str.contains(str(new_record["Time of Record"])).any():
            df = pd.concat([df, pd.DataFrame([new_record])], ignore_index=True)

            # Delete old file from S3 before uploading the new version
            s3_hook.delete_objects(bucket=S3_BUCKET, keys=[csv_filename])

            # Upload updated CSV to S3
            s3_hook.load_string(df.to_csv(index=False), key=csv_filename, bucket_name=S3_BUCKET)
    else:
        # Create new CSV and upload if it does not exist
        df = pd.DataFrame([new_record])
        s3_hook.load_string(df.to_csv(index=False), key=csv_filename, bucket_name=S3_BUCKET)

# Load Latest Data to Snowflake via Stage
def load_to_snowflake(city):
    snowflake_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    table_name = f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{city.lower()}"
    stage_area = f"@{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_STAGE}"

    copy_sql = f"""
    COPY INTO {table_name} (
    City, 
    Description, 
    Temperature, 
    Feels_Like, 
    Min_Temperature, 
    Max_Temperature, 
    Pressure, 
    Humidity, 
    Wind_Speed, 
    Time_of_Record, 
    Sunrise, 
    Sunset
    )
    FROM '{stage_area}/{city.lower()}.csv'
    FILE_FORMAT = (FORMAT_NAME = cities_weather_database.cities_weather_schema.csv_format)
    ON_ERROR = 'CONTINUE';
    """

    snowflake_hook.run(copy_sql)

# Slack Notification on Failure
def slack_alert(context):
    # Get Webhook URL from Airflow Variables (or hardcode if needed)

    # Build Message
    message = f"""
    Task Failed
    DAG: {context['task_instance'].dag_id}
    Task: {context['task_instance'].task_id}
    Execution Date: {context['execution_date']}
    Log URL: <{context['task_instance'].log_url}|View Logs>
    """

    # Send Message to Slack
    payload = {"text": message, "channel": "#all-dummy-weather-team"}
    response = requests.post(SLACK_WEBHOOK_URL, json=payload)

    # Log Response (Optional)
    if response.status_code != 200:
        raise ValueError(f"Slack notification failed: {response.text}")
    
# Slack Notification on Success
slack_success_notification = SlackWebhookOperator(
    task_id="tsk_slack_success",
    slack_webhook_conn_id="slack_new_conn",
    message="""
    Weather ETL Pipeline Completed Successfully!
    Dag: weather_etl_to_snowflake
    """,
    channel="#all-dummy-weather-team",
    trigger_rule="all_success"  # Runs only if all tasks succeed
)

# Default DAG Args
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 3, 11),
    'retries': 1,
    'retry_delay': timedelta(seconds=3),
    'on_failure_callback': slack_alert
}

# Define DAG
with DAG('weather_api_s3_snowflake_slack_etl',
         default_args=default_args,
         schedule_interval=timedelta(minutes=5),
         catchup=False) as dag:

    extract_transform_tasks = []
    load_to_snowflake_tasks = []

    for city in CITIES:
        extract_transform_task = PythonOperator(
            task_id=f'tsk_extract_transform_{city.lower()}',
            python_callable=extract_transform_weather_data,
            op_kwargs={'city': city}
        )

        load_to_snowflake_task = PythonOperator(
            task_id=f'tsk_load_to_snowflake_{city.lower()}',
            python_callable=load_to_snowflake,
            op_kwargs={'city': city}
        )

        extract_transform_task >> load_to_snowflake_task

        extract_transform_tasks.append(extract_transform_task)
        load_to_snowflake_tasks.append(load_to_snowflake_task)

    # Ensure Slack Notification runs after all city ETL tasks are done
    load_to_snowflake_tasks >> slack_success_notification
