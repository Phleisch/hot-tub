FROM python:3.7-alpine

# Install Python libs
RUN pip install requests
RUN pip install kafka-python

# Copy Python file and execute
ADD . /usr/src/app/
WORKDIR /usr/src/app

# Start streaming application
CMD python3 open_weather_api.py
