import os
import numpy as np
from pathlib import Path
from scipy.interpolate import griddata
from scipy.ndimage.filters import gaussian_filter
import json
from geopy.geocoders import Nominatim
from kafka import KafkaConsumer
from pyspark import SparkContext
from pyspark.sql import SQLContext


class BatchProcessor:
    """!@brief Kafka message triggered node to process the Cassandra table.

    Initializes a SparkSession, connects to Kafka, listenes to the 'triggerBatch' topic and executes the table
    processing once a threshold of trigger messages is exceeded. Data is read from Cassandra.
    @see https://cassandra.apache.org/.
    @see https://kafka.apache.org/.
    """

    def __init__(self):
        """!@brief BatchProcessor constructor.

        Exports necessary environment variables and initializes the SparkContext and SparkSQLContext.
        """
        self.ROOT_PATH = Path().resolve().parent
        self.msg_count = 0
        self.num_api_workers = 3
        self.key_space_name = 'hot_tub'
        self.table_name = 'current'
        self.processing_sigma = 30  # Value for gaussian filtering during model postprocessing. Modify if necessary.
        self.geolocator = Nominatim(user_agent="hot_tub")
        self.city_location_cache = self._load_city_location_cache()
        self.land_mask = np.load(self.ROOT_PATH.joinpath('data', 'mask_model.npy'))
        self.cache_dirty = False
        os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector_2.11:2.3.0 \
                                             --conf spark.cassandra.connection.host=127.0.0.1 pyspark-shell'
        self.sc = SparkContext()
        self.sql_context = SQLContext(self.sc)

    def start(self):
        """!@brief Starts the KafkaConsumer and sets up the processing trigger.

        The table is processed when the amount of messages received equals the number of API workers, effectively
        signaling that each API worker has finished its current work package.
        """
        consumer = KafkaConsumer('triggerBatch')
        for _ in consumer:
            self.msg_count = (self.msg_count + 1) % self.num_api_workers
            if self.msg_count == 0:
                self.batch_processing()

    def batch_processing(self):
        """!@brief Processes the table data from Cassandra and saves it back to Cassandra.

        Sequentially accesses all cities in the 'hot_tub.current' table, averages their temperature and saves the
        reduced data into """
        cities = self._load_rdd()
        avg_by_city = cities.mapValues(lambda v: (v, 1)).reduceByKey(lambda a, b: (a[0]+b[0], a[1]+b[1])) \
                                                        .mapValues(lambda v: v[0]/v[1]).collectAsMap()
        self.create_model(avg_by_city)

    def _load_rdd(self):
        rdd = self.sql_context.read.format("org.apache.spark.sql.cassandra").options(table=self.table_name,
                                                                                     keyspace=self.key_space_name) \
                                   .load().rdd.map(list).map(lambda x: (x[0], x[2]))
        return rdd

    def _load_city_location_cache(self):
        try:
            with open(self.ROOT_PATH.joinpath('src', 'batch_processing', 'city_cache.json'), 'r') as f:
                city_cache = json.load(f)
            return city_cache
        except FileNotFoundError:
            print("Error loading the cache. Cache not existent.")
            return dict()

    def _save_city_location_cache(self):
        try:
            with open(self.ROOT_PATH.joinpath('src', 'batch_processing', 'city_cache.json'), 'w') as f:
                city_cache = json.dump(self.city_location_cache, f)
            self.cache_dirty = False
            return city_cache
        except FileNotFoundError:
            print("Error writing the cache. Cache not existent.")

    def create_model(self, data):
        points = list()
        values = list()
        for city, temperature in data.items():
            if city in self.city_location_cache.keys():
                loc = self.city_location_cache[city]
                points.append([loc[0], loc[1]])
                values.append(temperature)
            else:
                try:
                    loc = self.geolocator.geocode({'city': city})
                    self.city_location_cache[city] = (loc.longitude, loc.latitude)
                    points.append([loc.latitude, loc.longitude])
                    values.append(temperature)
                    self.cache_dirty = True
                except:  # noqa: E722
                    pass
        if self.cache_dirty:
            self._save_city_location_cache()
        self._interpolate_model(points, values)

    def _interpolate_model(self, points, values):
        points.append([0, 0])
        values.append(2)
        x_grid, y_grid = np.mgrid[0:900, 0:1800]
        points = np.array(points)*5  # Upscaling from 90/180° to 450/900
        values = np.array(values)
        points_tf = np.zeros(points.shape)
        points_tf[:, 0] = points[:, 1] * -1 + 450
        points_tf[:, 1] = points[:, 0] + 900
        model = griddata(points_tf, values, (x_grid, y_grid), method='nearest')
        model = gaussian_filter(model, [self.processing_sigma, self.processing_sigma])
        model[~self.land_mask] = np.nan
        np.save(self.ROOT_PATH.joinpath('data', 'current_model'), model, allow_pickle=False)


if __name__ == '__main__':
    batch_processor = BatchProcessor()
    batch_processor.start()
