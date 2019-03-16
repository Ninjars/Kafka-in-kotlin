import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.databind.util.StdDateFormat
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import org.apache.kafka.common.serialization.Serializer

class TargetModelSerializer : Serializer<TargetModel> {
    private val jsonMapper = ObjectMapper().apply {
        registerKotlinModule()
        disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
        dateFormat = StdDateFormat()
    }

    override fun serialize(topic: String?, data: TargetModel?): ByteArray? {
        return data?.let{ jsonMapper.writeValueAsBytes(data) }
    }

    override fun configure(configs: MutableMap<String, *>?, isKey: Boolean) {}

    override fun close() {}
}

data class TargetModel(val sourceTime: Long, val data: String)
