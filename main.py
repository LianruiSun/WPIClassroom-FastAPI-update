from dotenv import load_dotenv
import os
import boto3
from decimal import Decimal
from datetime import datetime
from pytz import timezone
import requests
from concurrent.futures import ThreadPoolExecutor

load_dotenv()

# AWS credentials and region
ACCESS_KEY_ID = os.getenv("ACCESS_KEY_ID")
ACCESS_SECRET_KEY = os.getenv("ACCESS_SECRET_KEY")
REGION_NAME = os.getenv("REGION_NAME")

# Initialize DynamoDB resource
dynamodb = boto3.resource(
    "dynamodb",
    aws_access_key_id=ACCESS_KEY_ID,
    aws_secret_access_key=ACCESS_SECRET_KEY,
    region_name=REGION_NAME,
)

headers = {"x-api-key": os.getenv("API_KEY")}
base_url = "https://developer-apis.awair.is/v1/orgs/2674/devices"


def convert_celsius_to_fahrenheit(celsius):
    return (celsius * 9 / 5) + 32


def ensure_table(device_id, date_str):
    table_name = f"{device_id}_{date_str}"
    try:
        table = dynamodb.Table(table_name)
        table.load()  # Try to load the table, will throw an exception if not exists
    except dynamodb.meta.client.exceptions.ResourceNotFoundException:
        # Table does not exist, create new table
        table = dynamodb.create_table(
            TableName=table_name,
            KeySchema=[{"AttributeName": "timestamp", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "timestamp", "AttributeType": "S"}],
            ProvisionedThroughput={"ReadCapacityUnits": 1, "WriteCapacityUnits": 1},
        )
        table.wait_until_exists()  # Wait until the table is created
    return table


def get_air_data(device_id):
    url = f"{base_url}/awair-omni/{device_id}/air-data/latest"
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        data = response.json()
        if not data["data"]:  # Check if data list is empty
            print(f"No data available for device {device_id}\n")
        else:
            record = data["data"][0]
            # Convert timestamp to America/New_York timezone
            utc_time = datetime.strptime(record["timestamp"], "%Y-%m-%dT%H:%M:%S.%fZ")
            eastern = timezone("America/New_York")
            local_time = utc_time.replace(tzinfo=timezone("UTC")).astimezone(eastern)
            formatted_time = local_time.strftime("%Y-%m-%d %H:%M:%S")
            sensors = record["sensors"]
            sensor_dict = {sensor["comp"]: sensor["value"] for sensor in sensors}

            # Create item dictionary to store in DynamoDB
            sensor_item = {
                "timestamp": formatted_time,
                "pm10": Decimal(str(sensor_dict.get("pm10_est", 0))),
                "score": Decimal(str(sensor_dict.get("score", 0))),
                "temp": Decimal(
                    str(convert_celsius_to_fahrenheit(sensor_dict.get("temp", 0)))
                ),
                "humid": Decimal(str(sensor_dict.get("humid", 0))),
                "co2": Decimal(str(sensor_dict.get("co2", 0))),
                "voc": Decimal(str(sensor_dict.get("voc", 0))),
                "pm25": Decimal(str(sensor_dict.get("pm25", 0))),
                "noise": Decimal(str(sensor_dict.get("spl_a", 0))),
                "light": Decimal(str(sensor_dict.get("lux", 0))),
            }
            table = ensure_table(
                device_id, formatted_time.split(" ")[0].replace("-", "_")
            )
            table.put_item(Item=sensor_item)
            print(
                f"Data stored successfully for device {device_id} on date {formatted_time}\n"
            )
    else:
        print(f"Failed to fetch data for device {device_id}\n", response.status_code)


def lambda_handler(event, context):
    device_ids = ["15681", "15820", "15921", "16023", "16130", "16145", "16280", "16429", "16478", "16586"]
    with ThreadPoolExecutor(max_workers=10) as executor:
        executor.map(get_air_data, device_ids)

lambda_handler(None, None)