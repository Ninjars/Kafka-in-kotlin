
import com.xenomachina.argparser.ArgParser
import com.xenomachina.argparser.default
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import java.util.*

private const val CONSUME_TOPIC = "seed-topic"
private const val TARGET_TOPIC = "target-topic"
private const val DEFAULT_HOST = "localhost:9092"

private class GeneratorArgs(parser: ArgParser) {
    val broker by parser.positional("BROKER",
        help = "kafka broker for events, defaults to localhost:9092").default(DEFAULT_HOST)
    val consumeTopic by parser.storing("-c", "--consume",
        help = "Topic to consume events from").default { CONSUME_TOPIC }
    val targetTopic by parser.storing("-t", "--target",
        help = "Topic to emit processed events to").default { TARGET_TOPIC }
}

fun main(args: Array<String>) {
    ArgParser(args).parseInto(::GeneratorArgs).run {
        println("Initialising processor")
        val consumer = createKafkaConsumer(broker)
        val producer = createKafkaProducer(broker)
        val service = createService()
        val processor = Processor(consumeTopic, targetTopic, consumer, producer, service)

        var running = true
        while (running) {
            running = processor.poll()
        }
        println("Ended processor")
    }
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
