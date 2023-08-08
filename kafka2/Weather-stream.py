# https://openweathermap.org/ 에서 실시간 서울 날씨 받아오기

import requests
import json
from json import dumps
from dotenv import load_dotenv
import os
# Kafka Modules
from kafka import KafkaProducer

# Load environment variables from .env
load_dotenv()




# producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda x:dumps(x).encode('utf-8'))
 
producer = KafkaProducer(
    acks=0, # 메시지 전송 완료에 대한 체크
    compression_type='gzip', # 메시지 전달할 때 압축(None, gzip, snappy, lz4 등)
    bootstrap_servers=['localhost:9092'], # 전달하고자 하는 카프카 브로커의 주소 리스트
    value_serializer=lambda x:dumps(x).encode('utf-8') # 메시지의 값 직렬화
)

def connect_to_endpoint():
    # Set up parameters
    city = "Seoul"
    lang = "kr"
    apikey = os.getenv("apikey")

    # Server address to request, units=metric (changed to Celsius)
    api = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={apikey}&lang={lang}&units=metric"
    # Make the API request
    response = requests.get(api)
    # Process the API response
    data = json.loads(response.text)

    if data:
        producer.send('new', value=data)
        print('Sent!')
        producer.flush()
    if response.status_code != 200:
        raise Exception(
            "Request returned an error: {} {}".format(
                response.status_code, response.text
            )
        )

if __name__ == "__main__":
    connect_to_endpoint()