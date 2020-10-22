import requests
import random
import sys
import os
from datetime import datetime
from kafka import KafkaProducer

# Retrieve the API key stored as an environment variable
OPEN_WEATHER_API_KEY = os.environ['OPEN_WEATHER_API_KEY']

# The URL to send API requests
API_URL = 'https://api.openweathermap.org/data/2.5/weather?'

request_parameters = {
	'id': 0,						# Unique City ID for which to get data
	'appid': OPEN_WEATHER_API_KEY,	# API key
	'units': 'metric'				# Return weather data in metric units
}

KAFKA_SERVER = 'localhost:9092'  # Docker container port exposed at 19092.
CLIENT_ID = 'real_api'

# Kafka topic where temperature messsages should be posted
KAFKA_TOPIC = 'currentTemp'

# The status code required to accept an API result
EXPECTED_API_STATUS_CODE = 200

# Object used for sending temperature update messages to a Kafka topic
producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER, client_id=CLIENT_ID)

def generate_id_list_randomly(num_cities=100, filename='all_city_ids.txt'):
	"""
	Randomly select num_cities number of unique city ids from the given file,
	create and return a list with the unique city ids.

	@param num_cities	Number of unique city ids to randomly select
	@param filename		File from which to select random city ids

	@return a list of city ids equal in length to num_cities
	"""

	# Create a list from which random city ids should be sampled
	all_city_ids = generate_id_list_from_file(filename)

	# Randomly and uniquely select num_cities number within range 0 to the
	# total number of city ids (length of all_city_ids)
	indices = random.sample(range(0, len(all_city_ids)), num_cities)

	# Use list of indices to generate a list of city ids
	random_city_ids = [all_city_ids[index] for index in indices]
	return random_city_ids

def generate_id_list_from_file(filename='saved_city_ids.txt'):
	"""
	Create and return a list of ids using the file indicated by the given
	filename. The file is expected to have format of a single integer per line.

	@param filename	Name of the file from which a list of ids will be created

	@return a list of city ids (integers)
	"""

	file_as_list = None

	with open(filename, 'r') as f:
		file_as_list = [int(line) for line in f]

	return file_as_list

def call_api_with_retries(num_retries=3):
	"""
	Attempt an API call until successful or until the retry limit has been
	reached. A successful API call is one that has a status code of 200. Return
	the result of the API call.

	@param num_retries	Maximum number of times to try an API call if the call
						is failing
	
	@return the result of the API call or None if the API call never succeeded
	"""

	for _ in range(0, num_retries):
		api_result = requests.get(API_URL, params=request_parameters)
	
		if api_result.status_code == EXPECTED_API_STATUS_CODE:
			return api_result.json()
	
	return None

def produce_kafka_message(api_result):
	"""
	From the result of a weather API call, produce a message to a Kafka topic
	with key of the format 'city name:city longitude:city latitude:hour of day'
	and value 'current city temperature'.

	@param api_result	Result of a weather API call, used to produce a message
	"""

	# Extract important the values of a city we care about from the API result
	# and convert to a string where necessary for evaluation as a single string
	# later on
	city_name = api_result['name']
	city_temp = str(api_result['main']['temp'])
	city_lon = str(api_result['coord']['lon'])
	city_lat = str(api_result['coord']['lat'])

	# From the date that the API response was created, we only want the hour of
	# the day, from 0 to 23
	result_hour = str(datetime.fromtimestamp(api_result['dt']).hour)

	# Join values to construct a key (described above) using colons, transform
	# to bytes as required by the KafkaProducer send function
	msg_key = str.encode(':'.join([city_name, city_lon, city_lat, result_hour]))
	msg_val = str.encode(city_temp)

	# Send a message to Kafka
	producer.send(KAFKA_TOPIC, key=msg_key, value=msg_val)

def produce_messages_for_city_ids(city_ids):
	"""
	Given a list of city ids, make API calls for each ID in order to retrieve
	current city weather information and then produce a Kafka message from that
	information.

	@param city_ids	List of city ids for which weather information should be
					retrieved via an API call to OpenWeatherMap
	"""

	for city_id in city_ids:
		# Set the id to that of the current city in the request parameters used
		# to make an API call
		request_parameters['id'] = city_id
		api_result = call_api_with_retries()
	
		# If the api_result is not None (API call was successful), produce a
		# Kafka message
		if api_result:
			produce_kafka_message(api_result)

	# Once done sending a message to Kafka with temperature data for every city
	# in the city_ids list, then produce a message to Kafka to invoke batch
	# processing of the sent temperature messsages
	producer.send("batch")

city_ids = generate_id_list_randomly()

while 1:
	produce_messages_for_city_ids(city_ids)
