import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.databind.util.StdDateFormat
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import org.apache.kafka.common.serialization.Deserializer

class SourceModelDeserializer : Deserializer<SourceModel> {
    private val jsonMapper = ObjectMapper().apply {
        registerKotlinModule()
        disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
        dateFormat = StdDateFormat()
    }

    override fun deserialize(topic: String?, data: ByteArray?): SourceModel? {
        return data?.let { jsonMapper.readValue(data, SourceModel::class.java) }
    }

    override fun configure(configs: MutableMap<String, *>?, isKey: Boolean) {}

    override fun close() {}
}

data class SourceModel(val executionTime: Long)
