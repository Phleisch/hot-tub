package generator

import java.util.{Date, Properties}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}
import kafka.producer.KeyedMessage

object APIEmulator extends App {
    val topic = "triggerBatch"
    val brokers = "localhost:9092"

    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "APIEmulator")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")    
    val producer = new KafkaProducer[String, String](props)

    while (true) {
        val data = new ProducerRecord[String, String](topic, "null", "null")
        producer.send(data)
        print("Emulator active... ")
        Thread.sleep(100)
    }

    producer.close()
}