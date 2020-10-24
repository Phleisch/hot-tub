import requests
import random
import os
from datetime import datetime
# from kafka import KafkaProducer


# Get a list with num_cities many entries defining the cities for which hourly
# weather data should be retrieved
def generate_id_list_randomly(num_cities=100, filename='all_city_ids.txt'):
    all_city_ids = convert_file_to_list(filename)
    indices = random.sample(range(0, len(all_city_ids)), num_cities)
    random_city_ids = [all_city_ids[index] for index in indices]
    return random_city_ids


def generate_id_list_from_file(filename='saved_city_ids.txt'):
    return convert_file_to_list(filename)


def convert_file_to_list(filename):
    file_as_list = list()
    with open(filename, 'r') as f:
        for line in f:
            file_as_list.append(int(line.strip()))
    return file_as_list


def call_api_with_retries(num_retries=3):
    for _ in range(0, num_retries):
        api_result = requests.get(API_URL, params=request_parameters)
        if api_result.status_code == EXPECTED_API_STATUS_CODE:
            return api_result.json()
    return


def produce_kafka_message(api_result):
    city_name = api_result['name']
    city_temp = str(api_result['main']['temp'])
    city_lon = str(api_result['coord']['lon'])
    city_lat = str(api_result['coord']['lat'])
    result_hour = str(datetime.fromtimestamp(api_result['dt']).hour)
    msg_key = str.encode(':'.join([city_name, city_lon, city_lat, result_hour]))
    msg_val = str.encode(city_temp)
    #producer.send(KAFKA_TOPIC, key=msg_key, value=msg_val)


def produce_messages_for_city_ids(city_ids):
    for city_id in city_ids:
        request_parameters['id'] = city_id
        api_result = call_api_with_retries()
        if api_result:
            produce_kafka_message(api_result)
    # producer.send("batch")


if __name__ == '__main__':
    # Retrieve the API key stored as an environment variable
    OPEN_WEATHER_API_KEY = os.environ['OPEN_WEATHER_API_KEY']

    # The URL to send API requests
    API_URL = 'https://api.openweathermap.org/data/2.5/weather?'

    request_parameters = {
        'id': 0,						# Unique City ID for which to get data
        'appid': OPEN_WEATHER_API_KEY,  # API key
        'units': 'metric'				# Return weather data in metric units
    }

    KAFKA_SERVER = 'localhost:9092'
    CLIENT_ID = 'real_api'
    KAFKA_TOPIC = 'currentTemp'

    EXPECTED_API_STATUS_CODE = 200

    # producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER, client_id=CLIENT_ID)

    city_ids = generate_id_list_randomly()

    while True:
        produce_messages_for_city_ids(city_ids)
