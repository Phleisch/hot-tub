# Webserver functionality and dynamic web pages
from flask import Flask, render_template

# Retrieving numerical information about the current day
from datetime import datetime

# Converting numpy array to an image to be displayed on a webpage
from PIL import Image

# Loading a numpy array representing a temperature difference map
from model_service.model_service import ModelService

# Location where an image representation of the map numpy array is saved
temperature_map_location = 'static/global_temp_difference_map.png'

# Must create an instance of the ModelService class to properly use the
# load_model method
model_service = ModelService()

app = Flask(__name__)

# How requests for the route (127.0.0.1:5000) should be handled
@app.route('/')
def index(temperature_map=temperature_map_location):
	
	# Get current numbered day of the year, indexed at 1
	day_of_year = datetime.now().timetuple().tm_yday - 1
	
	# Get a numpy array, storing data of an interpolated map from the
	# difference of current and historical temperature averages
	temp_map_as_numpy_array = model_service.load_model(day_of_year)

	# Convert the numpy array to an image memory
	img = Image.fromarray(temp_map_as_numpy_array)	
	
	# Save the image memory as an image file as determined by the file
	# extension (PNG)
	img.save(temperature_map)

	return render_template('index.html', temperature_map=temperature_map)

# How requests for the route (127.0.0.1:5000/about) should be handled
@app.route('/about')
def about():
	return render_template('about.html')
