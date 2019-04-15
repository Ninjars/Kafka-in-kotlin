
import com.xenomachina.argparser.ArgParser
import com.xenomachina.argparser.default
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.serialization.StringSerializer
import java.util.*

private const val SEED_TOPIC = "seed-topic"
private const val DEFAULT_HOST = "localhost:9092"

private class GeneratorArgs(parser: ArgParser) {
    val count by parser.storing("-c", "--count",
        help = "number of emitted events, negative number for unlimited, defaults to 1") { toLong() }.default(1)
    val interval by parser.storing("-i", "--interval",
        help = "milliseconds between emitted events, defaults to 100ms") { toLong() }.default(100)
    val broker by parser.positional("BROKER",
        help = "kafka broker for events, defaults to localhost:9092").default(DEFAULT_HOST)
    val topic by parser.storing("-e", "--emit",
        help = "Topic to emit events to").default { SEED_TOPIC }
}

fun main(args : Array<String>) {
    ArgParser(args).parseInto(::GeneratorArgs).run {
        println("Emitting $count seed events to $broker at $interval millisecond intervals")
        val producer = createKafkaProducer(broker)
        val seedEventGenerator = SeedEventGenerator(topic, producer)
        seedEventGenerator.start(interval, count)
    }
}

fun createKafkaProducer(brokers: String): KafkaProducer<String, SeedEvent> {
    val props = Properties().apply {
        this["bootstrap.servers"] = brokers
    }
    return KafkaProducer<String, SeedEvent>(
        props,
        StringSerializer(),
        SeedEventSerializer()
    )
}
