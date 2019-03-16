
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import java.util.*

fun main(args: Array<String>) {
    println("Initialising processor")

    val brokers = "localhost:9092"
    val consumer = createKafkaConsumer(brokers)
    val producer = createKafkaProducer(brokers)
    val service = createService()
    val processor = Processor(consumer, producer, service)

    var running = true
    while (running) {
        running = processor.poll()
    }
    println("Ended processor")
}

private fun createKafkaConsumer(brokers: String): KafkaConsumer<String, SourceModel> {
    val consumerProps = Properties().apply {
        this["bootstrap.servers"] = brokers
        this["group.id"] = "group-id"
    }

    return KafkaConsumer<String, SourceModel>(
        consumerProps,
        StringDeserializer(),
        SourceModelDeserializer()
    )
}

private fun createKafkaProducer(brokers: String): KafkaProducer<String, TargetModel> {
    val props = Properties().apply {
        this["bootstrap.servers"] = brokers
    }
    return KafkaProducer<String, TargetModel>(
        props,
        StringSerializer(),
        TargetModelSerializer()
    )
}
