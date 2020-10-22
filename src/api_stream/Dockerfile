FROM python:3.7-alpine

# Setup environment keys
ARG api_key
ENV OPEN_WEATHER_API_KEY=$api_key

# Install Python libs
RUN pip install requests
RUN pip install kafka-python

# Copy Python file and execute
COPY open_weather_api.py /usr/src/app/open_weather_api.py
COPY all_city_ids.txt /usr/src/app/all_city_ids.txt
WORKDIR /usr/src/app
# Start streaming application
CMD python3 open_weather_api.py
