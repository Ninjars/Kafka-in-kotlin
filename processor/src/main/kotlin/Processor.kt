import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.log4j.LogManager
import java.time.Duration

class Processor(
    private val consumer: KafkaConsumer<String, SourceModel>,
    private val producer: KafkaProducer<String, TargetModel>,
    private val service: Service
) {

    private val logger = LogManager.getLogger(javaClass)

    private var count = 0

    init {
        consumer.subscribe(listOf(SEED_TOPIC))
    }

    fun poll(): Boolean {
        return try {
            val pollEvents = consumer.poll(Duration.ofSeconds(1))
            logger.debug("polling ${pollEvents.count()} new events")

            pollEvents.iterator().forEach {
                count++
                val event = it.value()
                val id = count
                GlobalScope.launch {
                    logger.info("> $id processing $event")
                    val planet = fetchPlanet(id)
                    logger.info("> $id planet: ${planet.name}")
                    val filmUrls = planet.films
                    val film = getFirstFilm(filmUrls)
                    logger.info("> $id film: ${film?.title}")
                    val emit = TargetModel(event.executionTime, id, planet.name, film?.title)
                    logger.info("emitting $emit")
                    producer.send(ProducerRecord(TARGET_TOPIC, emit))
                }
            }
            true

        } catch (e: Throwable) {
            logger.error("encountered an error", e)
            false
        }
    }

    private suspend fun getFirstFilm(films: List<String>): Film? {
        return if (films.isEmpty()) {
            null
        } else {
            service.getFilmFullUrl(films[0]).await()
        }
    }

    private suspend fun fetchPlanet(arg: Int): Planet {
        return service.getPlanet(arg).await()
    }
}
