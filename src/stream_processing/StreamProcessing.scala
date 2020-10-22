package sparkstreaming

import org.apache.spark.streaming.kafka._
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import scala.util.Random

import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._
import com.datastax.driver.core.{Session, Cluster, Host, Metadata}
import com.datastax.spark.connector.streaming._


/** Stream processing node saving Kafka messages to Cassandra.
 *
 * Messages are decoded and put into the hot_tub.current table. 
 * Table uses compound key to enable unique entries for each hour.
 */
object StreamProcessing {

    /** Main function of the StreamProcessing class.
     *
     * Serves as the entry point for the processing.
     *
     * @param args Required args string for the main function.
     */
    def main(args: Array[String]) {
        val cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
        val session = cluster.connect()
        session.execute("CREATE KEYSPACE IF NOT EXISTS hot_tub WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1};")
        session.execute("CREATE TABLE IF NOT EXISTS hot_tub.current (city text, time int, temperature float, PRIMARY KEY (city, time));")

        val conf = new SparkConf().setMaster("local[2]").setAppName("CurrentTemperatureProcessing")
        val ssc = new StreamingContext(conf, Seconds(1))
        ssc.checkpoint("./checkpoints/")
        val kafkaConf =  Map[String, String]("metadata.broker.list" -> "localhost:19092")  // Docker port 19092.
        val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaConf, Set("currentTemp"))

        val pairs = messages.map(x => (x._1, x._2.toDouble))

        /** Returns a tuple of (city, time, newTemp).
         *
         * Stateful stream mapping. Splits the key into the city name, the record time and combines the values to a 
         * single return tuple. Uses the old value if the optional value is not present. Incoming keys are of form 
         * (<Cityname>:<Time>, <Temperature>)
         *
         * @return A tuple of (city, time, newTemp).
         */
        def mappingFuncStream(key: String, value: Option[Double], state: State[Double]): (String, Integer, Double) = {
            val oldTemp = state.getOption.getOrElse(value.getOrElse(0.0))
            val newTemp = value.getOrElse(oldTemp)
            state.update(newTemp)
            val keySplit = key.split(":")
            val city = keySplit(0)
            val time = keySplit(1).toInt    
            (city, time, newTemp)
        }

        val stateDstream = pairs.mapWithState(StateSpec.function(mappingFuncStream _))

        // store the result in Cassandra
        stateDstream.saveToCassandra("hot_tub", "current", SomeColumns("city", "time", "temperature"))

        ssc.start()
        ssc.awaitTermination()
    }
}