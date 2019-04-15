
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.databind.util.StdDateFormat
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.Serializer
import org.apache.log4j.LogManager
import java.util.*
import kotlin.concurrent.schedule

class SeedEventGenerator(private val topic: String, private val producer: KafkaProducer<String, SeedEvent>) {
    private val logger = LogManager.getLogger(javaClass)

    private var timer: Timer? = null

    fun start(interval: Long, count: Long) {
        // Once cancelled, a Timer can't be restarted, so we have to create a new timer here.
        // This means that timer is a nullable type, as it may not have been assigned.
        // If we assign a value to `timer` then try to use it on the next line without null checking,
        // it will show a lint warning as the type itself is nullable.
        // `.apply()` is a Kotlin language feature that passes the value through to its inner function as the known type
        // which, given that the value we're calling it on is non-null, allows us to use it without null checking first.
        timer = Timer().apply {
            var iteration = 0
            schedule(0, interval) {
                val event = SeedEvent(scheduledExecutionTime())
                logger.info("creating new event $event")

                producer.send(ProducerRecord(topic, event)).get()
                if (count >= 0) {
                    iteration++
                    if (iteration >= count) {
                        stop()
                    }
                }
            }
        }
    }

    private fun stop() {
        timer?.cancel()
    }
}



class SeedEventSerializer : Serializer<SeedEvent> {

    private val jsonMapper = ObjectMapper().apply {
        registerKotlinModule()
        disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
        dateFormat = StdDateFormat()
    }

    override fun serialize(topic: String?, data: SeedEvent?): ByteArray? {
        return data?.let{ jsonMapper.writeValueAsBytes(data) }

        // the above return statement is functionally equivalent to this:
        //  return if (data == null) {
        //      null
        //  } else {
        //      jsonMapper.writeValueAsBytes(data)
        //  }
    }

    override fun configure(configs: MutableMap<String, *>?, isKey: Boolean) {}

    override fun close() {}
}



data class SeedEvent(val executionTime: Long)