import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.log4j.LogManager
import java.time.Duration

class Processor(
    private val consumer: KafkaConsumer<String, SourceModel>,
    private val producer: KafkaProducer<String, TargetModel>
) {

    private val logger = LogManager.getLogger(javaClass)

    private var count = 0

    init {
        consumer.subscribe(listOf(SEED_TOPIC))
    }

    fun poll() : Boolean {
        return try {
            val pollEvents = consumer.poll(Duration.ofSeconds(1))
            logger.debug("polling ${pollEvents.count()} new events")

            pollEvents.iterator().forEach {
                count++
                val event = it.value()
                logger.info("processing $event")
                val emit = TargetModel(event.executionTime, "event number: $count")
                logger.info("emitting $emit")
                producer.send(ProducerRecord(TARGET_TOPIC, emit))
            }
            true

        } catch (e: Throwable) {
            logger.error("encountered an error", e)
            false
        }
    }
}
