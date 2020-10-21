from flask import Flask, render_template

app = Flask(__name__)

template_image_location = 'static/icon.png'

@app.route('/')
def index(temperature_map=template_image_location):
	return render_template('index.html', temperature_map=temperature_map)

