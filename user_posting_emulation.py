import requests
from time import sleep
import random
import json
import sqlalchemy
from sqlalchemy import text
import yaml
from datetime import datetime


random.seed(100)

def converter(o):
    if isinstance(o, datetime):
        return o.isoformat()
    raise TypeError(f"object of type is not JSON serializable")

class AWSDBConnector:

    def __init__(self, config_path="db_creds.yaml"):

        with open(config_path, "r") as file:
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
engine = new_connector.create_db_connector()

invoke_url_pin = "https://51qgghrell.execute-api.us-east-1.amazonaws.com/my-prod/topics/0eaf46a0829f.pin"
invoke_url_geo = "https://51qgghrell.execute-api.us-east-1.amazonaws.com/my-prod/topics/0eaf46a0829f.geo"
invoke_url_user = "https://51qgghrell.execute-api.us-east-1.amazonaws.com/my-prod/topics/0eaf46a0829f.user"


def run_infinite_post_data_loop():
    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        

        with engine.connect() as connection:

            pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
            pin_selected_row = connection.execute(pin_string)
            
            for row in pin_selected_row:
                pin_result = dict(row._mapping)

            geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            geo_selected_row = connection.execute(geo_string)
            
            for row in geo_selected_row:
                geo_result = dict(row._mapping)

            user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
            user_selected_row = connection.execute(user_string)
            
            for row in user_selected_row:
                user_result = dict(row._mapping)

            print("pin_result:", pin_result)
            print("geo_result:", geo_result)
            print("user_result:", user_result)

            headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}
            pin_response = requests.post(
                invoke_url_pin,
                headers=headers,
                data=json.dumps({
                    "records": [
                        {
                        "value": pin_result
                        }
                    ]
                }, default=converter)
            )
            print("Pinterest Response Status:", pin_response.status_code)
            print("Pinterest Response Body:", pin_response.text) 


            headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}
            geo_response = requests.post(
                invoke_url_geo,
                headers=headers,
                data=json.dumps({
                    "records": [
                        {
                        "value": geo_result
                        }
                    ]
                }, default=converter)
            )
            print("Geolocation Response Status:", geo_response.status_code)
            print("Geolocation Response Body:", geo_response.text)


            headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}
            user_response = requests.post(
                invoke_url_user,
                headers=headers,
                data=json.dumps({
                    "records": [
                        {
                        "value": user_result
                        }
                    ]
                }, default=converter)
            )
            print("User Response Status:", user_response.status_code)
            print("User Response Body:", user_response.text)


if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')
    