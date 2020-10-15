package generator

import java.util.{Date, Properties}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}
import scala.util.Random
import kafka.producer.KeyedMessage

object ScalaProducerExample extends App {
    def getRandomVal: String = {
        ((Random.nextFloat() - 0.5) * 40).toString()
    }

    def getRandomKey: String = {
        val cityCollection = Array("Austin", "Munich")
        val time = Random.nextInt(24)
        val cityIndex = Random.nextInt(2)
        cityCollection(cityIndex) + ":" + Integer.toString(time)
    }

    // val events = 10000
    val topic = "currentTemp"
    val brokers = "localhost:9092"
    val rnd = new Random()

    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "StreamEmulator")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")    
    val producer = new KafkaProducer[String, String](props)

    while (true) {
        val data = new ProducerRecord[String, String](topic, getRandomKey, getRandomVal)
        producer.send(data)
        print(data + "\n")
        Thread.sleep(100)
    }

    producer.close()
}