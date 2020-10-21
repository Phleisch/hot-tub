from flask import Flask, render_template

app = Flask(__name__)

template_image_location = 'static/icon.png'

# How requests for the route (127.0.0.1:5000) should be handled
@app.route('/')
def index():
	return render_template('index.html', temperature_map=template_image_location)

# How requests for the route (127.0.0.1:5000/about) should be handled
@app.route('/about')
def about():
	return render_template('about.html')
