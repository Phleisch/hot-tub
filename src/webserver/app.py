# Webserver functionality and dynamic web pages
from flask import Flask, render_template

from pathlib import Path


PATH = Path().joinpath('static', 'global_temp_diff_map.png')

app = Flask(__name__)

# How requests for the route (127.0.0.1:5000) should be handled
@app.route('/')
def index():
	return render_template('index.html', temperature_map=PATH)

# How requests for the route (127.0.0.1:5000/about) should be handled
@app.route('/about')
def about():
	return render_template('about.html')
