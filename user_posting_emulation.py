import requests
from time import sleep
import random
import json
import sqlalchemy
from sqlalchemy import text
import yaml
from datetime import datetime
import configparser


random.seed(100)

def converter(o):
    """
    The function `converter` converts a datetime object to its ISO format for JSON serialization.
    
    :param o: The `o` parameter in the `converter` function is used to represent an object that we want
    to convert to a JSON serializable format. The function checks if the object `o` is an instance of
    the `datetime` class. If it is, the function returns the ISO formatted string of
    :return: The `converter` function is returning the ISO formatted string of the datetime object `o`
    if `o` is an instance of the `datetime` class. If `o` is not a datetime object, it raises a
    TypeError with a message indicating that the object is not JSON serializable.
    """
    if isinstance(o, datetime):
        return o.isoformat()
    raise TypeError(f"object of type is not JSON serializable")

class AWSDBConnector:

    def __init__(self, config_path: str ="db_creds.yaml"):

        with open(config_path, "r", encoding="utf-8") as file:
            config = yaml.safe_load(file)

        db_config = config["database"]
        self.HOST = db_config["host"]
        self.USER = db_config["user"]
        self.PASSWORD = db_config["password"]
        self.DATABASE = db_config["database"]
        self.PORT = db_config["port"]
        
    def create_db_connector(self):
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4")
        return engine


new_connector = AWSDBConnector()
db_engine = new_connector.create_db_connector()


def select_random_row_from_table(
            table_name: str, 
            random_row: int, 
            conn: sqlalchemy.engine
        ) -> dict:
    
    query = text(f"SELECT * FROM {table_name} LIMIT {random_row}, 1")
    response = conn.execute(query)
    for row in response:
        result = dict(row._mapping)
    return result


def send_payload_to_api(headers: dict, payload: dict, endpoint: str, method_type: str = "POST"):
            
    response = requests.request(
        method_type,
        endpoint,
        headers=headers,
        data=payload,
        timeout=60
        )
    print("Response Status:", response.status_code)
    print("Response Body:", response.text)
    return response


def gen_kinesis_payload(data, stream_name, partition_key):
    data=json.dumps({
        "StreamName": stream_name,
        "Data": data,
        "PartitionKey": partition_key
    },  default=converter)
    return data


def gen_batch_payload(data):
    data=json.dumps({
        "records": [
            {
                "value": data
            }
        ]
    }, default=converter)
    return data


config = configparser.ConfigParser()
config.read("config.ini")

headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}


def run_infinite_post_data_loop():
    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)

        with db_engine.connect() as db_conn:
            
            user_row = select_random_row_from_table("user_data", random_row, db_conn)
            pin_row = select_random_row_from_table("pin_data", random_row, db_conn)
            geo_row = select_random_row_from_table("geo_data", random_row, db_conn)
            
            print("pin_result:", pin_row)
            print("geo_result:", geo_row)
            print("user_result:", user_row)


            batch_user_payload = gen_batch_payload(user_row)
            batch_pin_payload = gen_batch_payload(pin_row)
            batch_geo_payload = gen_batch_payload(geo_row)


            stream_user_payload = gen_kinesis_payload(user_row, "Kinesis-Prod-Stream", "user-partition")
            stream_pin_payload = gen_kinesis_payload(pin_row, "Kinesis-Prod-Stream", "pin-partition")
            stream_geo_payload = gen_kinesis_payload(geo_row, "Kinesis-Prod-Stream", "geo-partition")


            batch_pin_send = send_payload_to_api(headers, batch_payload, config.get("endpoints", "INVOKE_PIN_URL"))
            batch_geo_send = send_payload_to_api(headers, batch_payload, config.get("endpoints", "INVOKE_GEO_URL"))
            batch_user_send = send_payload_to_api(headers, batch_payload, config.get("endpoints", "INVOKE_USER_URL"))


            kinesis_endpoint = config.get("kinesis_endpoint", "endpoint")
            send_payload_to_api(headers, stream_user_payload)
            send_payload_to_api(headers, stream_pin_payload)
            send_payload_to_api(headers, stream_geo_payload)

if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')
    