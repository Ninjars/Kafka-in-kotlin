import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.launch
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.log4j.LogManager
import java.time.Duration

class Processor(
    subscribeTopic: String,
    private val targetTopic: String,
    private val consumer: KafkaConsumer<String, SourceModel>,
    private val producer: KafkaProducer<String, TargetModel>,
    private val service: Service
) {

    private val logger = LogManager.getLogger(javaClass)

    private var count = 0

    init {
        consumer.subscribe(listOf(subscribeTopic))
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
                    val films = getAllFilms(planet.films)
                    val filmTitles = films.joinToString { film -> film.title }
                    logger.info("> $id planet: ${planet.name}")
                    logger.info("> $id films: $filmTitles")
                    val emit = TargetModel(event.executionTime, id, planet.name, filmTitles)
                    logger.info("emitting $emit")
                    producer.send(ProducerRecord(targetTopic, emit))
                }
            }
            true

        } catch (e: Throwable) {
            logger.error("encountered an error", e)
            false
        }
    }

    private suspend fun getAllFilms(films: List<String>): List<Film> {
        return if (films.isEmpty()) {
            emptyList()
        } else {
            return films.map { service.getFilmFullUrl(it) }.awaitAll()
        }
    }

    private suspend fun fetchPlanet(arg: Int): Planet {
        return service.getPlanet(arg).await()
    }
}
