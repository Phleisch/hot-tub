package sparkstreaming

import java.util.HashMap
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka._
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.storage.StorageLevel
import java.util.{Date, Properties}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}
import scala.util.Random

import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._
import com.datastax.driver.core.{Session, Cluster, Host, Metadata}
import com.datastax.spark.connector.streaming._

object KafkaSpark {
  def main(args: Array[String]) {
    val cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
    val session = cluster.connect()
    session.execute("CREATE KEYSPACE IF NOT EXISTS temperature WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1};")
    session.execute("CREATE TABLE IF NOT EXISTS temperature.current (city_time text PRIMARY KEY, temperature float);")

    val conf = new SparkConf().setMaster("local[2]").setAppName("CurrentTemperatureProcessing")
    val ssc = new StreamingContext(conf, Seconds(1))
    ssc.checkpoint("./checkpoints/")
    val kafkaConf =  Map[String, String]("metadata.broker.list" -> "localhost:9092")
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaConf, Set("currentTemp"))

    val pairs = messages.map(x => (x._1, x._2.toDouble))


    def mappingFunc(key: String, value: Option[Double], state: State[Double]): (String, Double) = {
      val oldTemp = state.getOption.getOrElse(value.getOrElse(0.0))
      val newTemp = value.getOrElse(oldTemp)
      state.update(newTemp)
      (key, newTemp)
    }

    val stateDstream = pairs.mapWithState(StateSpec.function(mappingFunc _))

    // store the result in Cassandra
    stateDstream.saveToCassandra("temperature", "current", SomeColumns("city_time", "temperature"))

    ssc.start()
    ssc.awaitTermination()
  }
}